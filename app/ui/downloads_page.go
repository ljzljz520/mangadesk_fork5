package ui

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/darylhjd/mangadesk/app/core"
	"github.com/darylhjd/mangadesk/app/ui/utils"

	"github.com/darylhjd/mangodex"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// uiActive controls whether background goroutines may post to the tview event
// loop. It is flipped off before the application stops so that no callback can
// block waiting on a stopped interface.
var uiActive int32 = 1

// Deactivate : Stop delivering callbacks to the terminal UI.
func Deactivate() {
	atomic.StoreInt32(&uiActive, 0)
}

// queueDraw : Post a UI update only while the interface is still running.
func queueDraw(f func()) {
	if atomic.LoadInt32(&uiActive) == 0 {
		return
	}
	core.App.TView.QueueUpdateDraw(f)
}

// currentMangaPage is the mounted manga page. Pages share one page ID, so at
// most one exists at a time. It lets application-owned download tasks refresh
// the page without holding a direct reference to it.
var (
	mangaPageMu      sync.Mutex
	currentMangaPage *MangaPage
)

func registerMangaPage(p *MangaPage) {
	mangaPageMu.Lock()
	currentMangaPage = p
	mangaPageMu.Unlock()
}

func unregisterMangaPage(p *MangaPage) {
	mangaPageMu.Lock()
	if currentMangaPage == p {
		currentMangaPage = nil
	}
	mangaPageMu.Unlock()
}

// bindDownloadHooks : Connect the application download queue to the UI. Must be
// called once while the tview event loop is running.
func bindDownloadHooks() {
	core.App.Downloads.OnChange = func(_ *core.DownloadTask) {
		queueDraw(func() {
			updateOpenMangaPages()
			refreshDownloadsPage()
		})
	}
}

// updateOpenMangaPages : Mark freshly completed downloads on the open manga
// page. Cancelled or failed tasks never show a success marker.
func updateOpenMangaPages() {
	mangaPageMu.Lock()
	p := currentMangaPage
	mangaPageMu.Unlock()
	if p == nil {
		return
	}

	completed := map[string]struct{}{}
	for _, task := range core.App.Downloads.Tasks() {
		if task.Status == core.StatusCompleted && task.Request.MangaID == p.Manga.ID {
			completed[task.Request.ChapterID] = struct{}{}
		}
	}
	if len(completed) == 0 {
		return
	}

	for row := 1; row < p.Table.GetRowCount(); row++ {
		chapter, ok := p.Table.GetCell(row, 0).GetReference().(*mangodex.Chapter)
		if !ok {
			continue
		}
		if _, done := completed[chapter.ID]; !done {
			continue
		}
		downloadCell := tview.NewTableCell(readStatus).
			SetTextColor(utils.MangaPageDownloadStatColor)
		p.Table.SetCell(row, 2, downloadCell)
	}
}

// DownloadsPage : Lists every known download task and allows cancelling them.
type DownloadsPage struct {
	Grid  *tview.Grid
	Table *tview.Table
}

// ShowDownloadsPage : Make the app show the downloads page.
func ShowDownloadsPage() {
	// Re-creating replaces any previously mounted page (same page ID).
	downloadsPage = nil
	page := newDownloadsPage()

	core.App.TView.SetFocus(page.Grid)
	core.App.PageHolder.AddAndSwitchToPage(utils.DownloadsPageID, page.Grid, true)
}

// downloadsPage : Currently mounted downloads page, if any.
var downloadsPage *DownloadsPage

// newDownloadsPage : Create the downloads page.
func newDownloadsPage() *DownloadsPage {
	grid := utils.NewGrid([]int{-1}, []int{-1})
	grid.SetTitleColor(utils.DownloadsPageGridTitleColor).
		SetBorderColor(utils.DownloadsPageGridBorderColor).
		SetBorder(true)

	table := tview.NewTable()
	statusHeader := tview.NewTableCell("Status").
		SetTextColor(utils.DownloadsPageStatusColor).SetSelectable(false)
	mangaHeader := tview.NewTableCell("Manga").
		SetTextColor(utils.DownloadsPageMangaColor).SetSelectable(false)
	chapterHeader := tview.NewTableCell("Chapter").
		SetTextColor(utils.DownloadsPageChapterColor).SetSelectable(false)
	qualityHeader := tview.NewTableCell("Quality").
		SetTextColor(utils.DownloadsPageQualityColor).SetSelectable(false)
	formatHeader := tview.NewTableCell("Format").
		SetTextColor(utils.DownloadsPageFormatColor).SetSelectable(false)
	table.SetCell(0, 0, statusHeader).
		SetCell(0, 1, mangaHeader).
		SetCell(0, 2, chapterHeader).
		SetCell(0, 3, qualityHeader).
		SetCell(0, 4, formatHeader).
		SetFixed(1, 0)
	table.SetSelectable(true, false).
		SetSeparator('|').
		SetBordersColor(utils.DownloadsPageTableBorderColor).
		SetBorder(true)

	grid.AddItem(table, 0, 0, 1, 1, 0, 0, true)

	page := &DownloadsPage{Grid: grid, Table: table}
	downloadsPage = page
	page.setHandlers()
	page.refresh()

	return page
}

func (p *DownloadsPage) setHandlers() {
	p.Grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			core.App.PageHolder.RemovePage(utils.DownloadsPageID)
			downloadsPage = nil
		case tcell.KeyCtrlX:
			p.cancelSelected()
		}
		switch event.Rune() {
		case 'c', 'C':
			p.cancelSelected()
		case 'r', 'R':
			core.App.Downloads.Prune()
			p.refresh()
		}
		return event
	})
}

// cancelSelected : Cancel the task under the cursor. Only queued or running
// tasks can be cancelled; the in-flight request is aborted.
func (p *DownloadsPage) cancelSelected() {
	row, _ := p.Table.GetSelection()
	key, ok := p.Table.GetCell(row, 0).GetReference().(string)
	if !ok {
		return
	}
	if !core.App.Downloads.Cancel(key) {
		return
	}
	log.Printf("Cancelled download task %s.\n", key)
	p.refresh()
}

// refresh : Rebuild the task table from manager state.
func (p *DownloadsPage) refresh() {
	tasks := core.App.Downloads.Tasks()

	// Preserve the cursor across live updates where possible.
	selectedRow, selectedCol := p.Table.GetSelection()

	var waiting, running int
	for _, task := range tasks {
		switch task.Status {
		case core.StatusQueued:
			waiting++
		case core.StatusRunning:
			running++
		}
	}
	p.Grid.SetTitle(fmt.Sprintf(" Downloads (limit: %d, active: %d, waiting: %d) | c/Ctrl+X cancel | r clear finished | Esc back ",
		core.App.Downloads.Concurrency(), running, waiting))

	p.Table.Clear()
	p.Table.SetCell(0, 0, tview.NewTableCell("Status").
		SetTextColor(utils.DownloadsPageStatusColor).SetSelectable(false)).
		SetCell(0, 1, tview.NewTableCell("Manga").
			SetTextColor(utils.DownloadsPageMangaColor).SetSelectable(false)).
		SetCell(0, 2, tview.NewTableCell("Chapter").
			SetTextColor(utils.DownloadsPageChapterColor).SetSelectable(false)).
		SetCell(0, 3, tview.NewTableCell("Quality").
			SetTextColor(utils.DownloadsPageQualityColor).SetSelectable(false)).
		SetCell(0, 4, tview.NewTableCell("Format").
			SetTextColor(utils.DownloadsPageFormatColor).SetSelectable(false))

	for i, task := range tasks {
		row := i + 1
		statusCell := tview.NewTableCell(fmt.Sprintf("%-12s", task.Status.String())).
			SetTextColor(statusColor(task.Status)).SetReference(task.Key)

		chapterName := fmt.Sprintf("%s - %s", task.Request.ChapterNumber, task.Request.ChapterTitle)
		format := "images"
		if task.Request.AsZip {
			format = task.Request.ZipType
		}

		p.Table.SetCell(row, 0, statusCell).
			SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%-30s", task.Request.MangaTitle)).
				SetMaxWidth(30).SetTextColor(utils.DownloadsPageMangaColor)).
			SetCell(row, 2, tview.NewTableCell(fmt.Sprintf("%-40s", chapterName)).
				SetMaxWidth(40).SetTextColor(utils.DownloadsPageChapterColor)).
			SetCell(row, 3, tview.NewTableCell(task.Request.Quality).
				SetTextColor(utils.DownloadsPageQualityColor)).
			SetCell(row, 4, tview.NewTableCell(format).
				SetTextColor(utils.DownloadsPageFormatColor))
	}

	if len(tasks) > 0 {
		if selectedRow < 1 || selectedRow > len(tasks) {
			selectedRow = 1
		}
		p.Table.Select(selectedRow, selectedCol)
	}
}

// refreshDownloadsPage : Refresh the downloads page if it is currently mounted.
func refreshDownloadsPage() {
	if downloadsPage == nil {
		return
	}
	if !core.App.PageHolder.HasPage(utils.DownloadsPageID) {
		downloadsPage = nil
		return
	}
	downloadsPage.refresh()
}

// statusColor : Colour for a status cell.
func statusColor(status core.DownloadStatus) tcell.Color {
	switch status {
	case core.StatusQueued:
		return tcell.ColorLightGrey
	case core.StatusRunning:
		return tcell.ColorLightSkyBlue
	case core.StatusCompleted:
		return tcell.ColorMediumSpringGreen
	case core.StatusFailed:
		return tcell.ColorIndianRed
	case core.StatusCancelled:
		return tcell.ColorOrange
	default:
		return tcell.ColorWhite
	}
}
