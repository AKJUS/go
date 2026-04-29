// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package commands

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/provisionutil"
	"maunium.net/go/mautrix/format"
)

var CommandImportImagePack = &FullHandler{
	Func: fnImportImagePack,
	Name: "import-image-pack",
	Help: HelpMeta{
		Section:     HelpSectionMisc,
		Description: "Import a sticker or emoji pack from the remote network",
		Args:        "<url>",
	},
	RequiresLogin: true,
	NetworkAPI:    NetworkAPIImplements[bridgev2.StickerImportingNetworkAPI],
}

func fnImportImagePack(ce *Event) {
	login, _, args := getClientForStartingChat[bridgev2.StickerImportingNetworkAPI](ce, "importing pack")
	if len(args) == 0 {
		ce.Reply("Usage: `$cmdprefix import-image-pack <url>`")
		return
	}
	resp, err := provisionutil.ImportImagePack(ce.Ctx, login, args[0], true)
	if err != nil {
		ce.Reply("Failed to import pack: %s", err)
		return
	}
	ce.Reply(
		"Successfully bridged image pack to %s",
		format.MarkdownLink("your personal filtering space",
			resp.(*provisionutil.RespImagePackSavedToRoom).RoomID.URI(ce.Bridge.Matrix.ServerName()).MatrixToURL()))
}
