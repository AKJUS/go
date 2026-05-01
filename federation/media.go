// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package federation

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
)

type FileMetadata struct {
	// nothing here yet
}

type mediaPartReader struct {
	respBody io.ReadCloser
	part     *multipart.Part
}

func (mpr *mediaPartReader) Read(p []byte) (n int, err error) {
	return mpr.part.Read(p)
}

func (mpr *mediaPartReader) Close() error {
	err1 := mpr.part.Close()
	err2 := mpr.respBody.Close()
	return cmp.Or(err1, err2)
}

func (c *Client) DownloadMedia(ctx context.Context, serverName, mediaID string) (meta *FileMetadata, data io.ReadCloser, err error) {
	_, resp, err := c.MakeFullRequest(ctx, RequestParams{
		ServerName:   serverName,
		Method:       http.MethodGet,
		Path:         URLPath{"v1", "media", "download", mediaID},
		Authenticate: true,
		DontReadBody: true,
	})
	if err != nil {
		return nil, nil, nil
	}
	defer func() {
		if data == nil {
			_ = resp.Body.Close()
		}
	}()
	mimeType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse content type: %w", err)
	} else if mimeType != "multipart/mixed" {
		return nil, nil, fmt.Errorf("unexpected content type: %s", mimeType)
	} else if params["boundary"] == "" {
		return nil, nil, fmt.Errorf("missing boundary parameter in content type")
	}
	mr := multipart.NewReader(resp.Body, params["boundary"])
	part, err := mr.NextPart()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read metadata chunk: %w", err)
	} else if !strings.HasPrefix(part.Header.Get("Content-Type"), "application/json") {
		_ = part.Close()
		return nil, nil, fmt.Errorf("unexpected content type for metadata chunk: %s", part.Header.Get("Content-Type"))
	}
	mbr := http.MaxBytesReader(nil, part, 64*1024)
	err = json.NewDecoder(mbr).Decode(&meta)
	_ = mbr.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
	}
	part, err = mr.NextPart()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read data chunk: %w", err)
	}
	redir := part.Header.Get("Location")
	if redir != "" {
		_ = part.Close()
		_ = resp.Body.Close()
		data, err = c.downloadMediaRedirect(ctx, redir)
		return
	}
	return meta, &mediaPartReader{
		respBody: resp.Body,
		part:     part,
	}, nil
}

var EnableMediaDownloadRedirects = false

func (c *Client) downloadMediaRedirect(ctx context.Context, url string) (io.ReadCloser, error) {
	// TODO remove this option after there's an IP blacklist for SSRF protection
	if !EnableMediaDownloadRedirects {
		return nil, fmt.Errorf("media download redirects are disabled")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare redirect request: %w", err)
	}
	resp, err := c.ExtHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send redirect request: %w", err)
	} else if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code from redirect: %d", resp.StatusCode)
	}
	return resp.Body, nil
}
