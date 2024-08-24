package lanzou

import (
	"context"
	"errors"
	"fmt"
	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/errs"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/internal/op"
	"github.com/alist-org/alist/v3/internal/stream"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/alist-org/alist/v3/pkg/utils/random"
	"github.com/go-resty/resty/v2"
	"io"
	"math"
	"net/http"
	"os"
	stdpath "path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LanZou struct {
	Addition
	model.Storage
	uid string
	vei string

	flag int32

	flatInstance *LanZou
}

const maxFileNameLength = 95
const maxFolderNameLength = 45
const maxDescLength = 160

var fakeSuffix bool = os.Getenv("ALIST_LANZOU_FAKE_SUFFIX") == "1"
var repairListInfo bool = os.Getenv("ALIST_LANZOU_REPAIT_LIST_INFO") == "1"

func (d *LanZou) Config() driver.Config {
	config.NoOverwriteUpload = os.Getenv("ALIST_LANZOU_NO_OVERWRITE_UPLOAD") == "1"
	return config
}

func (d *LanZou) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *LanZou) Init(ctx context.Context) (err error) {
	switch d.Type {
	case "account":
		_, err := d.Login()
		if err != nil {
			return err
		}
		fallthrough
	case "cookie":
		if d.RootFolderID == "" {
			d.RootFolderID = "-1"
		}
		d.vei, d.uid, err = d.getVeiAndUid()
	}

	d.LongFileName = os.Getenv("ALIST_LANZOU_LONG_FILE_NAME") == "1"

	mountPath := utils.FixAndCleanPath(d.MountPath)

	flatRootDir := strings.TrimSpace(os.Getenv("ALIST_LANZOU_FLAT_ROOT_DIR"))
	if flatRootDir != "" {
		flatRootDir = utils.FixAndCleanPath(flatRootDir)
		if len(flatRootDir)-len(strings.ReplaceAll(flatRootDir, "/", "")) != 1 {
			if strings.HasPrefix(flatRootDir, utils.PathAddSeparatorSuffix(mountPath)) {
				flatRootDir = strings.TrimPrefix(flatRootDir, mountPath)
			} else {
				flatRootDir = ""
			}
		}
	}
	d.FlatRootDir = flatRootDir
	if d.FlatRootDir != "" {
		jsonStr, err := utils.Json.Marshal(d)
		if err != nil {
			return err
		}
		d.flatInstance = &LanZou{}
		err = utils.Json.Unmarshal(jsonStr, d.flatInstance)
		if err != nil {
			return err
		}
		d.flatInstance.Cookie = d.Cookie
		d.flatInstance.vei = d.vei
		d.flatInstance.uid = d.uid
		d.flatInstance.MountPath, err = utils.JoinBasePath(d.flatInstance.MountPath,
			".flat.root."+utils.GetMD5EncodeStr(d.FlatRootDir)[:8])
	}
	return
}

func (d *LanZou) Drop(ctx context.Context) error {
	d.uid = ""
	if d.flatInstance != nil {
		d.flatInstance.uid = ""
	}
	return nil
}

var fakeSuffixReg = regexp.MustCompile(`^(.+)\._fk((\.s(\w+?)s)?(\.t(\w+?)t)?)\.(.+?)$`)

func formatFakeFullName(name string, ext string, size *int64, modTime *time.Time) string {
	if !fakeSuffix {
		return name
	}
	name += "._fk"
	if size != nil {
		name += fmt.Sprintf(".s%ss", toBase63(*size))
	}
	if modTime != nil {
		name += fmt.Sprintf(".t%st", toBase63(modTime.Unix()))
	}
	name += ext
	return name
}

func (d *LanZou) repairFileInfo(ctx context.Context, file model.Obj) (dfile *FileOrFolderByShareUrl, err error) {
	switch file := file.(type) {
	case *FileOrFolder:
		if file.repairFlag {
			return
		}
		if file.IsDir() {
			if fakeSuffix || d.RepairFileInfo {
				file.Name = unescapeFileName(file.Name)
				file.NameAll = file.Name
			}
			file.repairFlag = true
			return
		}
		file.Name = file.NameAll
	case *FileOrFolderByShareUrl:
		if file.repairFlag {
			return
		}
		if file.IsDir() {
			if fakeSuffix || d.RepairFileInfo {
				file.NameAll = unescapeFileName(file.NameAll)
			}
			file.repairFlag = true
			return
		}
	}
	name := file.GetName()
	var pSize *int64 = nil
	var pModtime *time.Time = nil
	needRename := fakeSuffix
	if fakeSuffix {
		strs := fakeSuffixReg.FindStringSubmatch(name)
		if strs != nil {
			name = strs[1]
			if strs[2] != "" {
				if size, e := fromBase63(strs[4]); e == nil {
					pSize = &size
				}
				if t, e := fromBase63(strs[6]); e == nil {
					modtime := time.Unix(t, 0)
					pModtime = &modtime
				}
				if pSize != nil && pModtime != nil {
					needRename = false
				}
			}
		}
	}
	if pSize == nil || pModtime == nil {
		var shareID, pwd string
		switch file := file.(type) {
		case *FileOrFolder:
			sfile := file.GetShareInfo()
			if sfile == nil {
				sfile, err = d.getFileShareUrlByID(file.GetID())
				if err != nil {
					return nil, err
				}
				file.SetShareInfo(sfile)
			}
			shareID = sfile.FID
			pwd = sfile.Pwd

		case *FileOrFolderByShareUrl:
			shareID = file.GetID()
			pwd = file.Pwd
		}
		dfile, err = d.GetFilesByShareUrl(shareID, pwd)
		if dfile != nil {
			var size *int64
			size, pModtime = d.getFileRealInfo(dfile.Url)
			if !(pSize != nil && size != nil && *pSize == 0 && *size == 1) {
				pSize = size
			}
		}
	}
	nameAll := name
	repairFlag := false
	if pSize != nil && pModtime != nil {
		repairFlag = true
	}
	if pSize != nil || pModtime != nil {
		if needRename {
			name = formatFakeFullName(name, stdpath.Ext(name), pSize, pModtime)
		}
	} else {
		needRename = false
	}
	nameAll = unescapeFileName(nameAll)
	switch file := file.(type) {
	case *FileOrFolder:
		file.NameAll = nameAll
		file.Name = file.NameAll
		if pSize != nil {
			file.size = pSize
		}
		if pModtime != nil {
			file.time = pModtime
		}
		file.repairFlag = repairFlag
	case *FileOrFolderByShareUrl:
		file.NameAll = nameAll
		if pSize != nil {
			file.size = pSize
		}
		if pModtime != nil {
			file.time = pModtime
		}
		file.repairFlag = repairFlag
	}
	if needRename {
		if d.IsAccount() || d.IsCookie() {
			go d.rename(ctx, file.(*FileOrFolder).parentId, file.GetID(), file.IsDir(), name)
		}
	}
	return dfile, nil
}

type FileNamePartInfo struct {
	file    model.Obj
	partMap map[int64]*[]FileNamePart
	version int64
}

type FileNamePart struct {
	partFile model.Obj
	index    int
	namePart string
	last     bool
}

func (d *LanZou) processDirEntries(ctx context.Context, dir model.Obj, dirEntries []model.Obj,
	callback func(file model.Obj) error) (result []model.Obj, err error) {
	if callback == nil {
		callback = func(file model.Obj) error { return nil }
	}
	fileMap := make(map[string]*FileNamePartInfo)
	repair := fakeSuffix || (repairListInfo && d.RepairFileInfo)
	n := len(dirEntries)
	for i := 0; i < n; {
		file := dirEntries[i]
		if !file.IsDir() && file.GetName() == "请忽使用第三方工具" {
			return nil, fmt.Errorf(file.GetName())
		}
		switch file := file.(type) {
		case *FileOrFolder:
			file.parentId = dir.GetID()
		case *FileOrFolderByShareUrl:
			file.parentId = dir.GetID()
		}
		if d.LongFileName {
			i++
			if !file.IsDir() && file.GetSize() == 1 {
				if strs := longFilenamePartReg.FindStringSubmatch(file.GetName()); strs != nil {
					fileId := strs[1]
					version, err := fromBase63(strs[2])
					if err != nil {
						continue
					}
					partInfo := fileMap[fileId]
					if partInfo == nil {
						partInfo = &FileNamePartInfo{}
						fileMap[fileId] = partInfo
					}
					if partInfo.partMap == nil {
						partInfo.partMap = make(map[int64]*[]FileNamePart)
					}
					index, _ := strconv.Atoi(strs[4])
					partList := partInfo.partMap[version]
					if partList == nil {
						partList = &[]FileNamePart{}
					}
					*partList = append(*partList, FileNamePart{
						partFile: file,
						index:    index,
						namePart: strs[5],
						last:     strs[3] == "e",
					})
					partInfo.partMap[version] = partList
					continue
				}
			}
			fileMap[file.GetID()] = &FileNamePartInfo{
				file: file,
			}
		} else {
			if repair {
				d.repairFileInfo(ctx, file)
			}
			if file.GetID() == "" {
				n--
				if i != n {
					dirEntries[i] = dirEntries[n]
				}
				continue
			}
			if err = callback(file); err != nil {
				return nil, err
			}
			i++
		}
	}
	if !d.LongFileName {
		return dirEntries[:n], nil
	}
	dirEntries = dirEntries[:0]
	var dirtyFiles [][]FileNamePart
	for _, fileInfo := range fileMap {
		if fileInfo.partMap == nil {
			continue
		}
		for version, parts := range fileInfo.partMap {
			if parts == nil {
				continue
			}
			partList := *parts
			sort.Slice(partList, func(i, j int) bool {
				if partList[i].index < partList[j].index {
					return true
				}
				if partList[i].index == partList[j].index {
					return !partList[i].partFile.ModTime().Before(partList[j].partFile.ModTime())
				}
				return false
			})
			invalid := false
			for i := 0; i < len(partList); i++ {
				if partList[i].index != i {
					invalid = true
					break
				}
			}
			if invalid || !partList[len(partList)-1].last {
				dirtyFiles = append(dirtyFiles, partList)
				continue
			}
			if version > fileInfo.version {
				fileInfo.version = version
			}
		}
	}
	for _, fileInfo := range fileMap {
		file := fileInfo.file
		if fileInfo.partMap != nil {
			partList := fileInfo.partMap[fileInfo.version]
			if partList != nil && len(*partList) > 0 {
				var nameBuilder strings.Builder
				for _, part := range *partList {
					nameBuilder.WriteString(part.namePart)
				}
				name := nameBuilder.String()
				switch file := file.(type) {
				case *FileOrFolder:
					file.NameAll = name
					file.Name = file.NameAll
				case *FileOrFolderByShareUrl:
					file.NameAll = name
				}
				delete(fileInfo.partMap, fileInfo.version)
				for _, parts := range fileInfo.partMap {
					dirtyFiles = append(dirtyFiles, *parts)
				}
			}
		}
		if file != nil {
			d.repairFileInfo(ctx, file)
			if err = callback(file); err != nil {
				break
			}
			dirEntries = append(dirEntries, file)
		}
	}
	for _, fileLists := range dirtyFiles {
		for _, file := range fileLists {
			go d.remove(ctx, false, file.partFile.GetID())
		}
	}
	if err != nil {
		return nil, err
	}
	return dirEntries, nil
}

func (d *LanZou) List(ctx context.Context, dir model.Obj, args model.ListArgs) (result []model.Obj, err error) {
	if d.IsCookie() || d.IsAccount() {
		if d.flatInstance != nil {
			return d.listFlatDir(ctx, dir, args)
		}
	}
	result, err = d.list(ctx, dir.GetID())
	if err != nil {
		return
	}
	if fakeSuffix || d.LongFileName || (repairListInfo && d.RepairFileInfo) {
		result, err = d.processDirEntries(ctx, dir, result, nil)
	}
	return
}

func (d *LanZou) list(ctx context.Context, dirId string) (result []model.Obj, err error) {
	if d.IsCookie() || d.IsAccount() {
		return d.GetAllFiles(dirId)
	} else {
		return d.GetFileOrFolderByShareUrl(dirId, d.SharePassword)
	}
}

func (d *LanZou) GetRoot(ctx context.Context) (file model.Obj, err error) {
	rootId := "-1"
	if d.flatInstance != nil {
		if (d.IsAccount() || d.IsCookie()) && d.FlatRootDir != "/" {
			if err = op.MakeDir(ctx, d.flatInstance, d.FlatRootDir); err != nil {
				return
			}
		}
		rootDir, err := op.GetUnwrap(ctx, d.flatInstance, d.FlatRootDir)
		if err != nil {
			return nil, err
		}
		rootId = rootDir.GetID()
	}
	if err != nil {
		return nil, err
	}
	if d.IsAccount() || d.IsCookie() {
		return &FileOrFolder{
			FolID:      rootId,
			NameAll:    "root",
			repairFlag: true,
		}, nil
	} else {
		return &FileOrFolderByShareUrl{
			ID:         rootId,
			NameAll:    "root",
			IsFloder:   true,
			repairFlag: true,
		}, nil
	}
}

var flatSubDirReg = regexp.MustCompile(
	`^(?P<del>\.deleted)?` +
		`\._fd(?P<type>[mis])_` +
		`((?P<tag>[0-9a-f]{3})|` +
		`((?P<id>\d+)_)?` +
		`(p(?P<pid>\d+)_)?` +
		`((?P<ftag>[0-9a-f]{6})_)?` +
		`(?P<name>.+?))$`)

func (d *LanZou) listFlatDir(ctx context.Context, dir model.Obj, args model.ListArgs) (result []model.Obj, err error) {
	var fileEntries []model.Obj
	if dirId, _ := strconv.Atoi(dir.GetID()); dirId <= 0 {
		fileEntries, err = op.List(ctx, d.flatInstance, d.FlatRootDir, args)
	} else {
		fileEntries, err = d.GetAllFiles(dir.GetID())
	}
	if err != nil {
		return nil, err
	}
	typeIndex := flatSubDirReg.SubexpIndex("type")
	tagIndex := flatSubDirReg.SubexpIndex("tag")
	idIndex := flatSubDirReg.SubexpIndex("id")
	pidIndex := flatSubDirReg.SubexpIndex("pid")
	ftagIndex := flatSubDirReg.SubexpIndex("ftag")
	nameIndex := flatSubDirReg.SubexpIndex("name")
	_, err = d.processDirEntries(ctx, dir, fileEntries, func(f model.Obj) error {
		if obj, ok := f.(*model.ObjWrapName); ok {
			f = obj.Unwrap()
		}
		file := f.(*FileOrFolder)
		if !file.IsDir() {
			result = append(result, file)
			return nil
		}
		strs := flatSubDirReg.FindStringSubmatch(file.GetName())
		if strs == nil {
			result = append(result, file)
			return nil
		}
		if strs[flatSubDirReg.SubexpIndex("del")] != "" {
			return nil
		}
		file = &FileOrFolder{
			ID:         file.ID,
			FolID:      file.FolID,
			Name:       file.Name,
			NameAll:    file.NameAll,
			Size:       file.Size,
			Time:       file.Time,
			parentId:   dir.GetID(),
			size:       file.size,
			time:       file.time,
			shareInfo:  file.shareInfo,
			repairFlag: file.repairFlag,
		}
		dirType := strs[typeIndex]
		tag := strs[tagIndex]
		fileId := strs[idIndex]
		parentId := strs[pidIndex]
		hashTag := strs[ftagIndex]
		fileName := strs[nameIndex]
		if dirType == "m" { // ._fdm_2000000_p1000000_9a1d462b_dirName
			if tag == "" && fileId != "" && parentId != "" && hashTag != "" && fileName != "" {
				file.ID = fmt.Sprintf("%s-p%s", file.FolID, dir.GetID()) //复用此字段，因为删除时需要同时删除 fdm 目录
				file.FolID = fileId
				file.parentId = parentId
				file.shareInfo = nil
			}
		} else if dirType == "i" { //._fdi_cb42
			if tag != "" && fileId == "" && hashTag == "" && fileName == "" {
				return nil
			}
		} else if dirType == "s" { //._fds_2851215_59ff599f_dirName
			if tag == "" && fileId != "" && hashTag != "" && fileName != "" {
				return nil
			}
		}
		file.NameAll = fileName
		file.Name = fileName
		result = append(result, file)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *LanZou) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	dfile, err := d.repairFileInfo(ctx, file)
	if err != nil {
		return nil, err
	}
	if file.GetSize() == 0 {
		exp := 30 * time.Minute
		return &model.Link{
			MFile:      model.NewNopMFile(strings.NewReader("")),
			Expiration: &exp,
		}, nil
	}
	if dfile == nil {
		var shareID, pwd string
		switch file := file.(type) {
		case *FileOrFolder:
			// 先获取分享链接
			sfile := file.GetShareInfo()
			if sfile == nil {
				sfile, err = d.getFileShareUrlByID(file.GetID())
				if err != nil {
					return nil, err
				}
				file.SetShareInfo(sfile)
			}
			shareID = sfile.FID
			pwd = sfile.Pwd

		case *FileOrFolderByShareUrl:
			shareID = file.GetID()
			pwd = file.Pwd
		}
		// 然后获取下载链接
		dfile, err = d.GetFilesByShareUrl(shareID, pwd)
		if dfile == nil {
			return nil, err
		}
	}
	exp := GetExpirationTime(dfile.Url)
	return &model.Link{
		URL: dfile.Url,
		Header: http.Header{
			"Referer":    []string{"https://pc.woozooo.com"},
			"User-Agent": []string{base.UserAgent},
		},
		Expiration: &exp,
	}, nil
}

func (d *LanZou) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) (dest model.Obj, err error) {
	if !d.IsCookie() && !d.IsAccount() {
		return nil, errs.NotSupport
	}
	if fakeSuffix || d.RepairFileInfo {
		var err error
		dirName, err = escapeFileName(dirName)
		if err != nil {
			return nil, err
		}
	}
	if len(dirName) > maxFileNameLength {
		return nil, filenameTooLong
	}
	if d.flatInstance == nil {
		return d.makeDir(ctx, parentDir, dirName, "")
	}
	hashTag := utils.GetMD5EncodeStr(random.String(8) + dirName)[:6]
	markDirName := fmt.Sprintf(".deleted._fdm_%s_%s", hashTag, dirName)
	markDir, err := d.makeDir(ctx, parentDir, markDirName, "")
	if err != nil {
		return nil, err
	}
	realPath := utils.FixAndCleanPath(fmt.Sprintf("%s/._fdi_%s/._fds_%s_%s_%s",
		d.FlatRootDir, hashTag[0:3], markDir.GetID(), hashTag, dirName))
	if err = op.MakeDir(ctx, d.flatInstance, realPath); err == nil {
		dest, err = op.GetUnwrap(ctx, d.flatInstance, realPath)
		if err == nil && dest != nil {
			markDirName = strings.TrimPrefix(markDirName, ".deleted._fdm_")
			markDirName = fmt.Sprintf("._fdm_%s_p%s_%s", dest.GetID(), dest.(*FileOrFolder).parentId, markDirName)
			err = d.rename(ctx, parentDir.GetID(), markDir.GetID(), true, markDirName)
		}
	}
	if err != nil {
		go d.remove(ctx, true, markDir.GetID())
		if dest != nil {
			go d.remove(ctx, true, dest.GetID())
		}
		return nil, err
	}
	dir := dest.(*FileOrFolder)
	dest = &FileOrFolder{
		FolID:      dir.GetID(),
		Name:       dirName,
		NameAll:    dirName,
		Size:       dir.Size,
		Time:       dir.Time,
		parentId:   parentDir.GetID(),
		size:       dir.size,
		time:       dir.time,
		repairFlag: dir.repairFlag,
		shareInfo:  dir.shareInfo,
	}
	return dest, nil
}

func (d *LanZou) makeDir(ctx context.Context, parentDir model.Obj, dirName string, dirDesc string) (dir *FileOrFolder, err error) {
	name := dirName
	if len(name) > maxFolderNameLength {
		if !d.LongFileName {
			return nil, filenameTooLong
		}
		name = name[:maxFolderNameLength]
	}
	var data []byte
	data, err = d.doupload(func(req *resty.Request) {
		req.SetContext(ctx)
		req.SetFormData(map[string]string{
			"task":               "2",
			"parent_id":          parentDir.GetID(),
			"folder_name":        name,
			"folder_description": dirDesc,
		})
	}, nil)
	if err != nil {
		return nil, err
	}
	dir = &FileOrFolder{
		Name:     dirName,
		FolID:    utils.Json.Get(data, "text").ToString(),
		parentId: parentDir.GetID(),
	}
	if len(dirName) > maxFolderNameLength {
		err = d.putLongFilenameParts(ctx, parentDir.GetID(), getLongFilenameParts(dir.GetID(), dirName, true))
		if err != nil {
			d.remove(ctx, true, dir.GetID())
			return nil, err
		}
	}
	return dir, nil
}

func (d *LanZou) Move(ctx context.Context, srcObj model.Obj, dstDir model.Obj) (model.Obj, error) {
	if !d.IsCookie() && !d.IsAccount() {
		return nil, errs.NotSupport
	}
	if srcObj.IsDir() {
		return nil, errs.NotSupport
	}
	file := srcObj.(*FileOrFolder)
	if d.LongFileName {
		err := d.putLongFilenameParts(ctx, dstDir.GetID(), getLongFilenameParts(file.GetID(), file.GetName(), file.IsDir()))
		if err != nil {
			return nil, err
		}
	}
	_, err := d.doupload(func(req *resty.Request) {
		req.SetContext(ctx)
		req.SetFormData(map[string]string{
			"task":      "20",
			"folder_id": dstDir.GetID(),
			"file_id":   srcObj.GetID(),
		})
	}, nil)
	if err != nil {
		if d.LongFileName {
			d.clearLongFilenameParts(ctx, dstDir.GetID(), file.GetID(), false)
		}
		return nil, err
	}
	if d.LongFileName {
		d.clearLongFilenameParts(ctx, file.parentId, file.GetID(), false)
	}
	file.parentId = dstDir.GetID()
	return file, nil
}

func (d *LanZou) Rename(ctx context.Context, srcObj model.Obj, newName string) (dst model.Obj, err error) {
	if !d.IsCookie() && !d.IsAccount() {
		return nil, errs.NotSupport
	}
	if fakeSuffix || d.RepairFileInfo {
		newName, err = escapeFileName(newName)
		if err != nil {
			return nil, err
		}
	}
	file := srcObj.(*FileOrFolder)
	toName := newName
	if !srcObj.IsDir() {
		fromExt := stdpath.Ext(srcObj.GetName())
		toExt := stdpath.Ext(toName)
		if fakeSuffix {
			toName = formatFakeFullName(toName, fromExt, file.size, file.time)
		} else if toExt != fromExt {
			return nil, fmt.Errorf("modifying file extension is not allowed")
		}
	}
	err = d.rename(ctx, srcObj.(*FileOrFolder).parentId, srcObj.GetID(), srcObj.IsDir(), toName)
	if err != nil {
		return nil, err
	}
	file.NameAll = newName
	file.Name = newName
	return srcObj, nil
}

var filenameTooLong = errors.New("filename too long")

func (d *LanZou) rename(ctx context.Context, parentId string, fileId string, isDir bool, newName string) (err error) {
	toName := newName
	if isDir {
		if len(toName) > maxFolderNameLength {
			if !d.LongFileName {
				return filenameTooLong
			}
			toName = toName[:maxFolderNameLength]
		}
	} else {
		if len(toName) > maxFileNameLength {
			if !d.LongFileName {
				return filenameTooLong
			}
			toName = toName[:maxFileNameLength]
		}
	}
	if d.LongFileName {
		err = d.putLongFilenameParts(ctx, parentId, getLongFilenameParts(fileId, newName, isDir))
		if err != nil {
			return err
		}
		d.clearLongFilenameParts(ctx, parentId, fileId, true)
	}
	_, err = d.doupload(func(req *resty.Request) {
		req.SetContext(ctx)
		if isDir {
			req.SetFormData(map[string]string{
				"task":        "4",
				"folder_id":   fileId,
				"folder_name": toName,
				//"folder_description": desc,
			})
		} else {
			if len(toName) <= maxFileNameLength {
				toName = strings.TrimPrefix(toName, stdpath.Ext(toName))
			}
			req.SetFormData(map[string]string{
				"task":      "46",
				"file_id":   fileId,
				"file_name": toName,
				"type":      "2",
			})
		}
	}, nil)
	if d.LongFileName {
		return nil
	}
	return err
}

func (d *LanZou) Remove(ctx context.Context, obj model.Obj) error {
	if !d.IsCookie() && !d.IsAccount() {
		return errs.NotSupport
	}
	if obj.IsDir() {
		list, err := d.List(ctx, obj, model.ListArgs{})
		if err != nil {
			return err
		}
		for _, file := range list {
			if file.IsDir() {
				err = d.Remove(ctx, file)
			} else {
				err = d.remove(ctx, false, file.GetID())
			}
			if err != nil {
				return err
			}
		}
	}
	err := d.remove(ctx, obj.IsDir(), obj.GetID())
	if err != nil {
		return err
	}
	file := obj.(*FileOrFolder)
	defer func() {
		if d.LongFileName {
			d.clearLongFilenameParts(ctx, file.parentId, file.GetID(), false)
		}
	}()
	if d.flatInstance != nil && obj.IsDir() {
		pairs := strings.Split(file.ID, "-p")
		if len(pairs) != 2 {
			return nil
		}
		fdmId := pairs[0]
		fdmParentId := pairs[1]
		for i := 0; i < 3; i++ {
			err = d.remove(ctx, true, fdmId)
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return err
		}
		if d.LongFileName {
			d.clearLongFilenameParts(ctx, fdmParentId, fdmId, false)
		}
	}
	return nil
}

func (d *LanZou) remove(ctx context.Context, isDir bool, fileId string) error {
	if !d.IsCookie() && !d.IsAccount() {
		return errs.NotSupport
	}
	_, err := d.doupload(func(req *resty.Request) {
		req.SetContext(ctx)
		if isDir {
			req.SetFormData(map[string]string{
				"task":      "3",
				"folder_id": fileId,
			})
		} else {
			req.SetFormData(map[string]string{
				"task":    "6",
				"file_id": fileId,
			})
		}
	}, nil)
	return err
}

var longFilenamePartReg = regexp.MustCompile(`^\.lfn_(\d+)_v(\w+?)v_p(e?)(\d+)_(.+)\.s0s\.t(\w+?)t\.zip$`)

func getLongFilenameParts(fileId string, fileName string, isDir bool) (parts []string) {
	const prefixFmt = ".lfn_%s_v%sv_p%s%d_%s.s0s.t%st.zip"
	version := toBase63(time.Now().UnixMilli())
	temp := fmt.Sprintf(prefixFmt, fileId, version, "", 1, "", version)
	parts = splitByLength(fileName, int(math.Min(float64(maxFileNameLength-len(temp)-10), maxFileNameLength/2)))
	last := len(parts) - 1
	for i := 0; i < last; i++ {
		parts[i] = fmt.Sprintf(prefixFmt, fileId, version, "", i, parts[i], version)
	}
	parts[last] = fmt.Sprintf(prefixFmt, fileId, version, "e", last, parts[last], version)
	return
}

func (d *LanZou) putLongFilenameParts(ctx context.Context, parentId string, nameParts []string) (err error) {
	if parentId == "" {
		return errs.ObjectNotFound
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(nameParts))
	partFileIds := make([]string, len(nameParts))
	for i, name := range nameParts {
		go func(index int, partName string) {
			defer waitGroup.Done()
			file, e := d.put(ctx, parentId, partName, nil)
			if e != nil {
				err = e
			} else {
				partFileIds[index] = file.GetID()
			}
		}(i, name)
	}
	waitGroup.Wait()
	if err != nil {
		for _, partId := range partFileIds {
			if partId != "" {
				go d.remove(ctx, false, partId)
			}
		}
	}
	return err
}

func (d *LanZou) clearLongFilenameParts(ctx context.Context, parentId string, fileId string, latestReserved bool) error {
	if parentId == "" {
		return errs.ObjectNotFound
	}
	fileList, err := d.GetFiles(parentId)
	if err != nil {
		return err
	}
	var latestVersion int64
	var partFileMap map[int64][]string
	for _, file := range fileList {
		if file.IsDir() || file.GetSize() != 1 {
			continue
		}
		strs := longFilenamePartReg.FindStringSubmatch(file.GetName())
		if strs == nil {
			continue
		}
		if strs[1] != fileId {
			continue
		}
		if !latestReserved {
			go d.remove(ctx, false, file.GetID())
			continue
		}
		version, err := strconv.ParseInt(strs[2], 10, 64)
		if err != nil {
			continue
		}
		if version > latestVersion {
			latestVersion = version
		}
		if partFileMap == nil {
			partFileMap = make(map[int64][]string)
		}
		partFileMap[version] = append(partFileMap[version], file.GetID())
	}
	delete(partFileMap, latestVersion)
	for _, partIdList := range partFileMap {
		for _, partId := range partIdList {
			go d.remove(ctx, false, partId)
		}
	}
	return nil
}

func (d *LanZou) Put(ctx context.Context, dstDir model.Obj, fileStream model.FileStreamer, up driver.UpdateProgress) (file model.Obj, err error) {
	if !d.IsCookie() && !d.IsAccount() {
		return nil, errs.NotSupport
	}
	fileName := fileStream.GetName()
	if fakeSuffix || d.RepairFileInfo {
		fileName, err = escapeFileName(fileName)
		if err != nil {
			return nil, err
		}
	}
	var pSize *int64
	var pModTime *time.Time
	var reader io.Reader = fileStream
	if fakeSuffix {
		if fs, ok := fileStream.(*stream.FileStream); ok {
			size := fs.GetSize()
			modTime := fs.ModTime()
			pSize = &size
			pModTime = &modTime
		}
		fileName = formatFakeFullName(fileName, ".zip", pSize, pModTime)
		if pSize != nil && *pSize == 0 {
			reader = nil
		}
	}
	toName := fileName
	if len(toName) > maxFileNameLength {
		if !d.LongFileName {
			return nil, filenameTooLong
		}
		toName = toName[:maxFileNameLength]
	}
	file, err = d.put(ctx, dstDir.GetID(), toName, reader)
	if err != nil {
		return nil, err
	}
	if len(fileName) > maxFileNameLength {
		fileObj := file.(*FileOrFolder)
		fileObj.NameAll = fileName
		fileObj.Name = fileName
		err = d.putLongFilenameParts(ctx, dstDir.GetID(), getLongFilenameParts(file.GetID(), fileName, false))
		d.remove(ctx, false, file.GetID())
		if err != nil {
			return nil, err
		}
	}
	d.repairFileInfo(ctx, file)
	return file, nil
}

func (d *LanZou) put(ctx context.Context, parentId string, fileName string, reader io.Reader) (dst model.Obj, err error) {
	if reader == nil {
		reader = strings.NewReader(" ") //蓝奏不允许上传空文件，上传一个空格
	}
	var resp RespText[[]FileOrFolder]
	_, err = d._post(d.BaseUrl+"/html5up.php", func(req *resty.Request) {
		req.SetFormData(map[string]string{
			"task":           "1",
			"vie":            "2",
			"ve":             "2",
			"id":             "WU_FILE_0",
			"name":           fileName,
			"folder_id_bb_n": parentId,
		}).SetFileReader("upload_file", fileName, reader).SetContext(ctx)
	}, &resp, true)
	if err != nil {
		return nil, err
	}
	var file *FileOrFolder = &resp.Text[0]
	file.parentId = parentId
	return file, nil
}
