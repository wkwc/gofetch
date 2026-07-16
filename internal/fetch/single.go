package fetch

import "context"

// singleDownload is used when the server does not support Range:
// one worker streams the entire body at offset 0.
func (d *Downloader) singleDownload(ctx context.Context, total int64, completed []Task) error {
	f, err := allocateSparse(d.OutFile, total, d.ResumeEnabled)
	if err != nil {
		return err
	}
	defer f.Close()
	prog := newProgress(total)
	if total > 0 {
		for _, t := range uncompleted(Task{Start: 0, End: total - 1}, completed) {
			if err := d.runTask(ctx, nil, t, prog, f); err != nil {
				return err
			}
		}
	}
	return d.finalize(f, prog)
}
