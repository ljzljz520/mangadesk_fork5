package ui

import (
	"fmt"
	"log"

	"github.com/darylhjd/mangadesk/app/core"
	"github.com/darylhjd/mangadesk/app/ui/utils"

	"github.com/darylhjd/mangodex"
	"github.com/rivo/tview"
)

// submitDownloads : Freeze the selected chapters into download requests and
// hand them to the application-owned download queue. Identical requests that
// already have an active task are merged and do not start a second download.
func (p *MangaPage) submitDownloads(selection map[int]struct{}) {
	var added, merged int
	for index := range selection {
		chapter, ok := p.Table.GetCell(index, 0).GetReference().(*mangodex.Chapter)
		if !ok {
			continue
		}

		// Freeze all parameters at submission time. The task keeps running
		// with these values even after the page is left or config changes.
		req := core.DownloadRequest{
			MangaID:       p.Manga.ID,
			MangaTitle:    p.Manga.GetTitle("en"),
			ChapterID:     chapter.ID,
			ChapterNumber: chapter.GetChapterNum(),
			ChapterTitle:  chapter.GetTitle(),
			Language:      chapter.Attributes.TranslatedLanguage,

			Quality:      core.App.Config.DownloadQuality,
			ForcePort443: core.App.Config.ForcePort443,
			AsZip:        core.App.Config.AsZip,
			ZipType:      core.App.Config.ZipType,
			DownloadDir:  core.App.Config.DownloadDir,
		}

		if _, isNew := core.App.Downloads.Submit(req); isNew {
			added++
		} else {
			merged++
		}
	}

	var msg string
	switch {
	case added == 0:
		msg = "Selected chapter(s) are already queued or downloading."
	case merged == 0:
		msg = fmt.Sprintf("Queued %d chapter download(s).\nPress Ctrl+D to view the queue.", added)
	default:
		msg = fmt.Sprintf("Queued %d chapter download(s).\n%d were already active.\nPress Ctrl+D to view the queue.",
			added, merged)
	}

	core.App.TView.QueueUpdateDraw(func() {
		modal := okModal(utils.DownloadQueuedModalID, msg)
		ShowModal(utils.DownloadQueuedModalID, modal)
	})
}

// toggleReadMarkers : Toggle read status for selected chapters.
func (p *MangaPage) toggleReadMarkers(selection map[int]struct{}) {
	// Check if the user is logged in. If they are not, we tell them that they cannot toggle without logging in.
	if !core.App.Client.Auth.IsLoggedIn() {
		log.Printf("Attempted toggling read marker while not logged in. Informing user...")
		core.App.TView.QueueUpdateDraw(func() {
			modal := okModal(utils.NotLoggedInErrorModalID, "You need to log in to toggle read status!")
			ShowModal(utils.NotLoggedInErrorModalID, modal)
		})
		return
	}

	// For each selection, we separate into make-read, make-unread bins.
	var (
		readMap   = map[int]string{}
		unReadMap = map[int]string{}
	)
	for row := range selection {
		var (
			chapter *mangodex.Chapter
			ok      bool
		)
		// Get the chapter for this row.
		if chapter, ok = p.Table.GetCell(row, 0).GetReference().(*mangodex.Chapter); !ok {
			return
		}

		// Get the readMap/unread status, and split accordingly.
		statusCell := p.Table.GetCell(row, 4)
		if statusCell.Text == readStatus { // If it was originally readMap, we toggle to unread.
			unReadMap[row] = chapter.ID
		} else {
			readMap[row] = chapter.ID
		}
	}

	// Get the read and unread IDs.
	var (
		read   = make([]string, 0, len(readMap))
		unRead = make([]string, 0, len(unReadMap))
	)
	for _, readID := range readMap {
		read = append(read, readID)
	}
	for _, unReadID := range unReadMap {
		unRead = append(unRead, unReadID)
	}

	// Send the request.
	if _, err := core.App.Client.Chapter.SetReadUnreadMangaChapters(p.Manga.ID, read, unRead); err != nil {
		// Error sending request, tell the user.
		log.Printf("Unable to update read markers: %s\n", err.Error())
		core.App.TView.QueueUpdateDraw(func() {
			modal := okModal(utils.GenericAPIErrorModalID,
				"Error updating read markers.\n\nCheck log for details.")
			ShowModal(utils.GenericAPIErrorModalID, modal)
		})
		return
	}

	// Update the table
	for row := range readMap {
		readCell := tview.NewTableCell(readStatus).SetTextColor(utils.MangaPageReadStatColor)
		p.Table.SetCell(row, 4, readCell)
	}
	for row := range unReadMap {
		readCell := tview.NewTableCell("").SetTextColor(utils.MangaPageReadStatColor)
		p.Table.SetCell(row, 4, readCell)
	}

	// Show user that read status successfully toggled.
	core.App.TView.QueueUpdateDraw(func() {
		modal := okModal(utils.ToggleReadChapterModalID, "Toggled Successfully!")
		ShowModal(utils.ToggleReadChapterModalID, modal)
	})
}

// toggleFollowManga : Toggle follow/unfollow of a manga.
func (p *MangaPage) toggleFollowManga() {
	// Check if the user is logged in. If they are not, we tell them that they cannot toggle without logging in.
	if !core.App.Client.Auth.IsLoggedIn() {
		log.Printf("Attmpted toggling follow while not logged in. Informing user...")
		core.App.TView.QueueUpdateDraw(func() {
			modal := okModal(utils.NotLoggedInErrorModalID, "You need to log in to follow/unfollow a manga!")
			ShowModal(utils.NotLoggedInErrorModalID, modal)
		})
		return
	}

	// Check whether the manga is currently being followed or not.
	log.Println("Checking manga follow status...")
	following, err := core.App.Client.Manga.CheckIfMangaFollowed(p.Manga.ID)
	if err != nil {
		log.Printf("Error getting manga follow status: %s\n", err.Error())
		core.App.TView.QueueUpdateDraw(func() {
			modal := okModal(utils.GenericAPIErrorModalID,
				"Error checking manga follow status.\nCheck log for details.")
			ShowModal(utils.GenericAPIErrorModalID, modal)
		})
		return
	}

	// Show a follow/unfollow modal based on current follow status
	var (
		text          string
		confirmButton string
		fn            func()
	)
	if following {
		log.Println("Manga was followed.")
		text = "You are already following this manga.\n\nUnfollow manga?"
		confirmButton = "Unfollow"
	} else {
		log.Println("Manga was not followed.")
		text = "You are not currently following this manga.\n\nFollow manga?"
		confirmButton = "Follow"
	}

	// Set up the function to do.
	fn = func() {
		var (
			id    string
			modal *tview.Modal
		)
		// Toggle follow and set up the result modal.
		if _, err = core.App.Client.Manga.ToggleMangaFollowStatus(p.Manga.ID, !following); err != nil {
			id = utils.GenericAPIErrorModalID
			modal = okModal(utils.GenericAPIErrorModalID,
				"Error following/unfollowing manga.\nCheck log for details.")
		} else {
			log.Println("Successfully toggled following of manga.")
			id = utils.ToggleFollowMangaDoneModalID
			msg := "Successfully followed manga."
			if following {
				msg = "Successfully unfollowed manga."
			}
			modal = okModal(utils.ToggleFollowMangaDoneModalID, msg)
		}
		ShowModal(id, modal)
	}

	// Show the modal to confirm toggling of follow.
	core.App.TView.QueueUpdateDraw(func() {
		modal := confirmModal(utils.ToggleFollowMangaModalID, text, confirmButton, fn)
		ShowModal(utils.ToggleFollowMangaModalID, modal)
	})
}
