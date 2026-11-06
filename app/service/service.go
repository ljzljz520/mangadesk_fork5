package service

import (
	"log"

	"github.com/darylhjd/mangodex"
	"github.com/rivo/tview"

	"github.com/darylhjd/mangadesk/app/core"
	"github.com/darylhjd/mangadesk/app/ui"
)

// Start : Set up the application.
func Start() {
	// Create new app.
	core.App = &core.MangaDesk{
		Client:     mangodex.NewDexClient(),
		TView:      tview.NewApplication(),
		PageHolder: tview.NewPages(),
	}

	// Create the application-owned download queue.
	core.App.Downloads = core.NewDownloadManager(
		core.NewDexAtHome(), core.DefaultDownloadConcurrency)

	// Show appropriate screen based on restore session result.
	if err := core.App.Initialise(); err != nil {
		ui.ShowLoginPage()
	} else {
		ui.ShowMainPage()
	}
	log.Println("Initialised starting screen.")
	ui.SetUniversalHandlers()

	// Run the app.
	log.Println("Running app...")
	if err := core.App.TView.Run(); err != nil {
		log.Println(err)
	}
}

// Shutdown : Shutdown the application.
func Shutdown() {
	// Stop delivering UI callbacks before the download queue is drained, so a
	// task finishing during shutdown never blocks on a stopped event loop.
	ui.Deactivate()
	core.App.Shutdown()
}
