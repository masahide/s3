package main

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	region       = "ap-northeast-1"
	bucket       = ""
	acl          = "private"
	mimeType     = "application/octet-stream"
	changedRc    = 254
	failedRc     = 255
	okRc         = 0
	preCheck     = false
	dryrun       = false
	version      = ""
	profile      = "infra-common"
	show_version = false
	fullpath     = false
)

func showHelp() {
	fmt.Printf("Usage:\n %s [options] <command> ...\n", path.Base(os.Args[0]))
	fmt.Println(" options:")
	flag.PrintDefaults()
	fmt.Println(" command:")
	fmt.Println("  ls bucket")
	fmt.Println("  ls bucket/path/to/")
	fmt.Println("  up path/to/localfile bucket/path/to")
	fmt.Println("  dl bucket/path/to path/to/localfile")
	fmt.Println("  cat bucket/path/to")
	fmt.Println("  zcat bucket/path/to")
	//fmt.Println("  grep bucket/path/to")
}

type s3DlParam struct {
	S3 *s3.S3
	s3.GetObjectInput
	dest   string
	mkdir  bool
	dryrun bool
}

type s3UpParam struct {
	s3.PutObjectInput
	S3     *s3.S3
	src    string
	dryrun bool
}

func uploadFileD(S3 *s3.S3, req *s3.PutObjectInput) (*s3.PutObjectInput, *s3.PutObjectOutput, error) {

	md5hex, _, _, err := md5Sum(req.Body)
	if err != nil {
		return nil, nil, err
	}
	res, err := S3.PutObject(req)
	if err != nil {
		log.Printf("bucket.Put err:%v", err)
		return req, res, err
	}
	if res == nil {
		return req, res, fmt.Errorf("res is nil pointer")
	}
	if res.ETag == nil {
		return req, res, fmt.Errorf("res.ETag is nil pointer")
	}
	if len(*res.ETag) < 2 {
		return req, res, fmt.Errorf("*res.ETag is too short. It should have 2 characters or more")
	}
	etag := (*res.ETag)[1 : len(*res.ETag)-1]
	if md5hex != etag {
		return req, res, fmt.Errorf("md5 and ETag does not match. md5:%s ETag:%s", md5hex, etag)
	}
	return req, res, err
}

func md5Sum(r io.ReadSeeker) (md5hex string, md5b64 string, size int64, err error) {
	var offset int64
	offset, err = r.Seek(0, os.SEEK_CUR)
	if err != nil {
		return
	}
	defer r.Seek(offset, os.SEEK_SET)
	digest := md5.New()
	size, err = io.Copy(digest, r)
	if err != nil {
		return
	}
	sum := digest.Sum(nil)
	md5hex = hex.EncodeToString(sum)
	md5b64 = base64.StdEncoding.EncodeToString(sum)
	return
}
func listObjectsCallBack(S3 *s3.S3, req *s3.ListObjectsInput, dirCb func(*s3.CommonPrefix) error, objectCb func(*s3.Object) error) error {
	for {
		l, err := S3.ListObjects(req)
		if err != nil {
			return err // give up retry.
		}
		for _, cp := range l.CommonPrefixes {
			if err := dirCb(cp); err != nil {
				return err
			}
		}
		for _, object := range l.Contents {
			if err := objectCb(object); err != nil {
				return err
			}
		}
		if !aws.BoolValue(l.IsTruncated) {
			return nil
		}
		req.Marker = l.NextMarker
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if show_version {
		fmt.Printf("version: %s\n", version)
		return
	}
	flag.IntVar(&changedRc, "rc_changed", changedRc, "changed return code")
	flag.IntVar(&failedRc, "rc_failed", failedRc, "failed return code")
	flag.IntVar(&okRc, "rc_ok", okRc, "OK return code")
	flag.BoolVar(&show_version, "version", false, "show version")
	flag.BoolVar(&fullpath, "fullpath", false, "show fullpath")
	flag.StringVar(&region, "Region", region, "Region")
	flag.StringVar(&profile, "p", profile, "profile name")
	flag.StringVar(&acl, "ACL", acl, "ACL of upload file")
	flag.StringVar(&mimeType, "MimeType", mimeType, "MimeType of upload file")
	flag.BoolVar(&preCheck, "precheck", preCheck, "pre-check mode")
	flag.BoolVar(&dryrun, "dry", dryrun, "dryrun mode")
	flag.Parse()
	args := flag.Args()

	if flag.NArg() < 1 {
		showHelp()
		os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	var err error

	params := []string{}
	if len(args) >= 2 {
		params = args[1:]
	}

	cmd, ok := cmds[args[0]]
	if !ok {
		showHelp()
		return
	}
	S3 := s3.New(session.New(), &aws.Config{Region: aws.String(region)})
	if err = cmd(S3, params); err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

}

type Commands map[string]func(*s3.S3, []string) error

var cmds = Commands{
	"ls":   ls,
	"dl":   dl,
	"up":   up,
	"cat":  cat,
	"zcat": zcat,
}

func dl(S3 *s3.S3, params []string) error {
	if len(params) < 1 {
		return fmt.Errorf("dl command parameter is not enough.")
	}

	path := strings.Split(params[0], "/")
	bucket = path[0]
	dest := "."
	if len(params) >= 2 {
		dest = params[1]
	}
	if len(path) == 0 {
		return fmt.Errorf("dl command path is unknown.")
	}
	key := strings.TrimPrefix(params[0], bucket+"/")
	s3filename := filepath.Base(key)
	destpath := filepath.Dir(dest)
	destfilename := filepath.Base(dest)
	if destfilename == "." || destfilename == ".." || strings.HasSuffix(dest, "/") {
		destfilename = s3filename
	}

	if !preCheck {
		req := &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		}
		res, err := S3.GetObject(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		os.MkdirAll(destpath, 0755)
		var f *os.File
		f, err = os.Create(filepath.Join(destpath, destfilename))
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, res.Body)
		return err
	}
	cs, err := s3Download(s3DlParam{
		S3: S3,
		GetObjectInput: s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
		dest:   filepath.Join(destpath, destfilename),
		mkdir:  true,
		dryrun: dryrun,
	})
	printChecksum(cs)
	if err != nil {
		os.Exit(failedRc)
		return err
	}
	if cs.changed {
		os.Exit(changedRc)
	}
	os.Exit(okRc)
	return nil
}

func zcat(S3 *s3.S3, params []string) error {
	f, err := getObject(S3, params)
	if err != nil {
		return fmt.Errorf("zcat command %s", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	_, err = io.Copy(os.Stdout, gz)
	return err
}

func cat(S3 *s3.S3, params []string) error {
	f, err := getObject(S3, params)
	if err != nil {
		return fmt.Errorf("cat command %s", err)
	}
	defer f.Close()
	_, err = io.Copy(os.Stdout, f)
	return err
}

func getObject(S3 *s3.S3, params []string) (io.ReadCloser, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("parameter is not enough.")
	}

	path := strings.Split(params[0], "/")
	bucket = path[0]
	if len(path) == 0 {
		return nil, fmt.Errorf("path is unknown.")
	}
	key := strings.TrimPrefix(params[0], bucket+"/")

	req := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	res, err := S3.GetObject(req)
	if err != nil {
		return nil, err
	}
	return res.Body, err
}
func up(S3 *s3.S3, params []string) error {
	if len(params) < 2 {
		return fmt.Errorf("up command parameter is not enough.")
	}

	path := strings.Split(params[1], "/")
	bucket = path[0]
	key := strings.TrimPrefix(params[1], bucket+"/")
	if bucket == key {
		key = ""
	}
	src := params[0]
	if len(path) == 0 {
		return fmt.Errorf("up command path is unknown.")
	}
	srcfilename := filepath.Base(src)
	if srcfilename == "." || srcfilename == ".." || strings.HasSuffix(src, "/") {
		return fmt.Errorf("up command src path is unknown: %s", src)
	}
	if strings.HasSuffix(key, "/") || key == "" {
		key = filepath.Join(key, srcfilename)
	}
	if !preCheck {
		f, err := os.Open(src)
		if err != nil {
			return err
		}
		defer f.Close()
		req := &s3.PutObjectInput{
			ACL:    &acl,
			Body:   f,
			Bucket: &bucket,
			//ContentLength: &size,
			ContentType: &mimeType,
			Key:         &key,
		}
		_, _, err = uploadFileD(S3, req)
		return err
	}
	cs, err := s3Upload(s3UpParam{
		S3:  S3,
		src: srcfilename,
		PutObjectInput: s3.PutObjectInput{
			Key:         &key,
			ACL:         &acl, // public-read-write, public-read, private
			ContentType: &mimeType,
		},
		dryrun: dryrun,
	})
	printChecksum(cs)
	if err != nil {
		os.Exit(failedRc)
		return err
	}
	if cs.changed {
		os.Exit(changedRc)
	}
	os.Exit(okRc)
	return nil
}

func printChecksum(cs checksum) {
	fmt.Printf("changed %v\n", cs.changed)
	fmt.Printf("size %v\n", cs.size)
	fmt.Printf("local_md5 %v\n", cs.localMd5hex)
	fmt.Printf("s3_md5 %v\n", cs.s3Md5hex)
}

func ls(S3 *s3.S3, params []string) error {
	if len(params) == 0 {
		return listBucket(S3)
	}

	path := strings.Split(params[0], "/")
	bucket := path[0]
	prefix := ""
	if len(path) > 1 {
		prefix = strings.TrimPrefix(params[0], bucket+"/")
	}
	req := &s3.ListObjectsInput{
		Bucket:    &bucket,
		Delimiter: aws.String("/"),
		Prefix:    &prefix,
	}
	err := listObjectsCallBack(S3, req, func(cp *s3.CommonPrefix) error {
		printPath(*cp.Prefix)
		return nil
	}, func(o *s3.Object) error {
		printPath(*o.Key)
		return nil
	})
	return err
}

func printPath(p string) {
	if fullpath {
		fmt.Printf("%s\n", p)
		return
	}
	paths := strings.Split(p, "/")
	if paths[len(paths)-1] == "" {
		fmt.Printf("%s/\n", paths[len(paths)-2])
		return
	}
	fmt.Printf("%s\n", paths[len(paths)-1])
}

func listBucket(S3 *s3.S3) error {
	res, err := S3.ListBuckets(&s3.ListBucketsInput{})
	for _, b := range res.Buckets {
		fmt.Println(*b.Name)
	}
	return err
}

type checksum struct {
	changed     bool
	size        int64
	localMd5hex string
	s3Md5hex    string
}

func s3UploadCheck(s s3UpParam) (checksum, error) {
	return checkMD5(s.S3, s.src, *s.Bucket, *s.Key)
}

func s3downloadCheck(s s3DlParam) (checksum, error) {
	return checkMD5(s.S3, s.dest, *s.Bucket, *s.Key)
}

func s3Upload(s s3UpParam) (checksum, error) {
	cs, err := s3UploadCheck(s)
	if err != nil || !cs.changed || s.dryrun {
		return cs, err
	}
	f, err := os.Open(s.src)
	if err != nil {
		return cs, err
	}
	defer f.Close()
	var po *s3.PutObjectOutput
	if *s.ContentType == "" {
		*s.ContentType = "application/octet-stream"
	}
	_, po, err = uploadFileD(s.S3, &s.PutObjectInput)
	if err != nil {
		cs.changed = false
		return cs, err
	}
	if len(*po.ETag) < 3 {
		cs.changed = false
		return cs, fmt.Errorf("ETag  is too short. etag:%s", po.ETag)
	}

	cs.s3Md5hex = (*po.ETag)[1 : len(*po.ETag)-1]
	if cs.s3Md5hex != cs.localMd5hex {
		cs.changed = false
		return cs, fmt.Errorf("upload failed. etag:%s", cs.s3Md5hex)

	}
	return cs, nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func checkMD5(s3 *s3.S3, src, backet, key string) (checksum, error) {
	if !fileExists(src) {
		return checksum{changed: true}, nil
	}
	f, err := os.Open(src)
	if err != nil {
		return checksum{}, err
	}
	defer f.Close()
	return checks3md5(f, s3, bucket, key)
}
func checks3md5(f io.ReadSeeker, s *s3.S3, bucket, key string) (checksum, error) {
	var err error
	res := checksum{}
	res.localMd5hex, _, _, err = md5Sum(f)
	if err != nil {
		return res, err
	}
	if r, err := s.HeadObject(&s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err == nil {
		if len(*r.ETag) < 3 {
			return res, fmt.Errorf("ETag  is too short. etag:%s", r.ETag)
		}
		res.s3Md5hex = (*r.ETag)[1 : len(*r.ETag)-1]
		res.size = *r.ContentLength
		if res.s3Md5hex != res.localMd5hex {
			res.changed = true
		}
	}
	return res, err
}

func s3Download(s s3DlParam) (checksum, error) {
	cs, err := s3downloadCheck(s)
	if err != nil || !cs.changed || s.dryrun {
		return cs, err
	}
	if s.mkdir {
		dirPath := filepath.Dir(s.dest)
		if src, err := os.Stat(dirPath); err != nil || !src.IsDir() {
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return cs, err
			}
		}
	}

	resGetObj, err := s.S3.GetObject(&s.GetObjectInput)
	if err != nil {
		return cs, err
	}
	defer resGetObj.Body.Close()

	dist, err := os.OpenFile(s.dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return cs, err
	}
	defer dist.Close()
	n, err := io.Copy(dist, resGetObj.Body)
	if err != nil {
		return cs, err
	}
	cs.size = n
	cs.s3Md5hex = (*resGetObj.ETag)[1 : len(*resGetObj.ETag)-1]

	return cs, nil
}
