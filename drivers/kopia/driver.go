package kopia

import (
	"context"
	"fmt"
	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
	"net/http"
	"net/url"
	"slices"
	"time"
)

type Kopia struct {
	Addition
	model.Storage

	rootId   string
}

func (d *Kopia) Config() driver.Config {
	return config
}

func (d *Kopia) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Kopia) Init(ctx context.Context) error {
	go retry.Do(
		func() error {
			resp := SnapshotResponse{}
			_, err := d.request(http.MethodGet, "/api/v1/snapshots", func(req *resty.Request) {
				req.SetQueryParam("userName", d.UserName)
				req.SetQueryParam("host", d.Host)
				req.SetQueryParam("path", d.Path)

			}, &resp)
			if err != nil {
				return err
			}
			for i := len(resp.Snapshots) - 1; i >= 0; i-- {
				snapshot := resp.Snapshots[i]
				if !slices.Contains(snapshot.Retention, "incomplete") {
					if (d.Snapshot == "pin" && len(snapshot.Pins) > 0) || (d.Snapshot == "" || d.Snapshot == "latest") || d.Snapshot == snapshot.RootID {
						d.rootId = snapshot.RootID
						break
					}
				}
			}
			if d.rootId == "" {
				return fmt.Errorf("kopia snapshot: %s not found", d.Snapshot)
			}
			utils.Log.Infof("kopia load snapshot: %s", d.rootId)
			return nil
		},
		retry.Delay(time.Second),
		retry.Attempts(86400*365*1000),
		retry.MaxDelay(3200*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	)
	return nil
}

func (d *Kopia) Drop(ctx context.Context) error {
	return nil
}

func (d *Kopia) GetRoot(ctx context.Context) (result model.Obj, err error) {
	err = retry.Do(
		func() error {
			resp := FileResponse{}
			_, err := d.request(http.MethodGet, fmt.Sprintf("/api/v1/objects/%s", d.rootId), nil, &resp)
			if err != nil {
				return err
			}
			result = &model.Object{
				ID:       d.rootId,
				Path:     "/",
				IsFolder: true,
				Size:     resp.Summary.Size,
				Modified: resp.Summary.MaxTime,
			}
			return nil
		},
		retry.Delay(time.Second),
		retry.Attempts(200),
		retry.MaxDelay(3200*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Kopia) List(ctx context.Context, dir model.Obj, args model.ListArgs) (result []model.Obj, err error) {
	err = retry.Do(
		func() error {
			resp := FileResponse{}
			_, err = d.request(http.MethodGet, fmt.Sprintf("/api/v1/objects/%s", dir.GetID()), nil, &resp)
			if err != nil {
				return err
			}
			result, err = utils.SliceConvert(resp.Entries, func(file Entry) (model.Obj, error) {
				return &model.Object{
					ID:       file.Obj,
					Name:     file.Name,
					IsFolder: file.Type == "d",
					Modified: file.MTime,
					Size:     file.Size,
				}, nil
			})
			return err
		},
		retry.Delay(time.Second),
		retry.Attempts(200),
		retry.MaxDelay(3200*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	)
	return result, err
}

func (d *Kopia) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	params := url.Values{}
	params.Add("fname", file.GetName())
	exp := 24 * time.Hour * 365 * 200
	return &model.Link{
		URL:        fmt.Sprintf("%s/api/v1/objects/%s?%s", d.Url, file.GetID(), params.Encode()),
		Expiration: &exp,
	}, nil
}

func (d *Kopia) request(method string, url string, callback base.ReqCallback, resp interface{}) (*resty.Response, error) {
	req := base.RestyClient.R()
	if callback != nil {
		callback(req)
	}
	if resp != nil {
		req.SetResult(resp)
	}
	res, err := req.Execute(method, d.Url+url)
	if err != nil {
		return nil, err
	}
	return res, nil
}
