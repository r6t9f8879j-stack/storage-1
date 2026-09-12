// ost is the storaged admin/ops CLI. It talks to a storaged node's REST API.
//
// Environment:
//
//	OST_BASE   base URL (default http://127.0.0.1:5000)
//	OST_KEY    admin key (default STORAGED_ADMIN_KEY at runtime)
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"storaged/internal/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	base := envOr("OST_BASE", "http://127.0.0.1:5000")
	key := envOr("OST_KEY", os.Getenv("STORAGED_ADMIN_KEY"))
	c := client.New(base, key)

	cmd := os.Args[1]
	rest := os.Args[2:]
	var err error
	switch cmd {
	case "health", "status":
		err = cmdHealth(ctx, c, rest)
	case "buckets":
		err = cmdBuckets(ctx, c, rest)
	case "put":
		err = cmdPut(ctx, c, rest)
	case "get":
		err = cmdGet(ctx, c, rest)
	case "list":
		err = cmdList(ctx, c, rest)
	case "delete":
		err = cmdDelete(ctx, c, rest)
	case "gc":
		err = cmdGC(ctx, c, rest)
	case "checkpoint":
		err = cmdCheckpoint(ctx, c, rest)
	case "promote":
		err = cmdPromote(ctx, c, rest)
	case "demote":
		err = cmdDemote(ctx, c, rest)
	case "transfer":
		err = cmdTransfer(ctx, c, rest)
	case "transfers":
		err = cmdTransfers(ctx, c, rest)
	case "ls":
		err = cmdLs(ctx, c, rest)
	case "trash":
		err = cmdTrash(ctx, c, rest)
	case "restore":
		err = cmdRestore(ctx, c, rest)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ost:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `ost — storaged ops CLI

  ost health
  ost buckets                 list buckets
  ost buckets create NAME     create bucket (--public)
  ost buckets get NAME
  ost buckets delete NAME
  ost put BUCKET KEY FILE     upload a file (commits atomically)
  ost get BUCKET KEY FILE     download a file to disk
  ost list BUCKET             list objects (--prefix=)
  ost delete BUCKET KEY       delete an object
  ost gc                      run garbage collection
  ost checkpoint              truncate the WAL
  ost promote                 become writer (failover)
  ost demote                  step down as writer
  ost transfer BUCKET URL|MAGNET|FILE.torrent
                            fetch a URL, magnet link, or .torrent file (--key=)
  ost transfers BUCKET [cancel ID]
                            list, or cancel, transfer jobs
  ost ls BUCKET [PREFIX]      directory-style listing (folders + files)
  ost trash                   list trashed objects
  ost trash restore BUCKET KEY
  ost trash purge BUCKET KEY  permanently delete one
  ost trash purge-all         permanently delete all trashed
  ost restore BUCKET KEY      alias for trash restore

Environment: OST_BASE (default http://127.0.0.1:5000), OST_KEY.
`)
}

func cmdHealth(ctx context.Context, c *client.Client, args []string) error {
	h, err := c.Health(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "status:\t%s\nnode:\t%s\nrole:\t%s\nwrites:\t%v\nlsn:\t%d\nwatermark:\t%d\ndisk_free:\t%s\nactive_uploads:\t%d\n",
		h.Status, h.NodeID, h.Role, h.Writes, h.LSN, h.Watermark, human(h.DiskFree), h.ActiveUploads)
	return w.Flush()
}

func cmdBuckets(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		bs, err := c.ListBuckets(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tPUBLIC\tCREATED")
		for _, b := range bs {
			fmt.Fprintf(w, "%s\t%v\t%s\n", b.Name, b.IsPublic, b.CreatedAt)
		}
		return w.Flush()
	}
	switch args[0] {
	case "create":
		if len(args) < 2 {
			return fmt.Errorf("usage: ost buckets create NAME")
		}
		public := false
		for _, a := range args[2:] {
			if a == "--public" {
				public = true
			}
		}
		b, err := c.PutBucket(ctx, args[1], public)
		if err != nil {
			return err
		}
		fmt.Printf("created %s (public=%v)\n", b.Name, b.IsPublic)
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("usage: ost buckets get NAME")
		}
		b, err := c.GetBucket(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("%s public=%v created=%s\n", b.Name, b.IsPublic, b.CreatedAt)
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: ost buckets delete NAME")
		}
		if err := c.DeleteBucket(ctx, args[1]); err != nil {
			return err
		}
		fmt.Printf("deleted %s\n", args[1])
	default:
		return fmt.Errorf("unknown buckets subcommand %q", args[0])
	}
	return nil
}

func cmdPut(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: ost put BUCKET KEY FILE")
	}
	ct := "application/octet-stream"
	for i, a := range args {
		if a == "--content-type" && i+1 < len(args) {
			ct = args[i+1]
		}
	}
	obj, err := c.PutFile(ctx, args[0], args[1], args[2], ct, func(done, total int64) {
		fmt.Printf("\r  upload %s / %s", human(done), human(total))
	})
	if err != nil {
		return err
	}
	fmt.Printf("\ruploaded %s (%s) etag=%s\n", obj.Key, human(obj.Size), obj.ETag)
	return nil
}

func cmdGet(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: ost get BUCKET KEY FILE")
	}
	f, err := os.Create(args[2])
	if err != nil {
		return err
	}
	obj, err := c.GetFile(ctx, args[0], args[1], f)
	closeErr := f.Close()
	if err != nil {
		os.Remove(args[2])
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Printf("downloaded %s (%s) etag=%s\n", obj.Key, human(obj.Size), obj.ETag)
	return nil
}

func cmdList(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ost list BUCKET [--prefix=]")
	}
	prefix := ""
	limit := 1000
	for _, a := range args[1:] {
		if strings.HasPrefix(a, "--prefix=") {
			prefix = strings.TrimPrefix(a, "--prefix=")
		}
		if strings.HasPrefix(a, "--limit=") {
			limit, _ = strconv.Atoi(strings.TrimPrefix(a, "--limit="))
		}
	}
	entries, err := c.ListObjects(ctx, args[0], prefix, limit)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tSIZE\tETAG\tUPDATED")
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Key, human(e.Size), e.ETag, e.UpdatedAt)
	}
	return w.Flush()
}

func cmdLs(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ost ls BUCKET [PREFIX]")
	}
	bucket, prefix := args[0], ""
	if len(args) > 1 {
		prefix = args[1]
	}
	var folders []string
	var files []client.ListEntry
	after := ""
	for {
		f, d, next, err := c.ListFolder(ctx, bucket, prefix, after, 500)
		if err != nil {
			return err
		}
		files = append(files, f...)
		folders = append(folders, d...)
		if next == "" {
			break
		}
		after = next
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, d := range folders {
		// strip prefix for display
		disp := d
		if prefix != "" && strings.HasPrefix(d, prefix) {
			disp = d[len(prefix):]
		}
		fmt.Fprintf(w, "dir\t%s/\n", strings.TrimSuffix(disp, "/"))
	}
	fmt.Fprintln(w, "KEY\tSIZE\tETAG\tUPDATED")
	for _, e := range files {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Key, human(e.Size), e.ETag, e.UpdatedAt)
	}
	return w.Flush()
}

func cmdDelete(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: ost delete BUCKET KEY")
	}
	if err := c.DeleteObject(ctx, args[0], args[1]); err != nil {
		return err
	}
	fmt.Printf("deleted %s/%s\n", args[0], args[1])
	return nil
}

func cmdGC(ctx context.Context, c *client.Client, args []string) error {
	r, err := c.GC(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("gc: removed %d blobs, freed %s\n", r.BlobsRemoved, human(r.BytesFreed))
	return nil
}

func cmdCheckpoint(ctx context.Context, c *client.Client, args []string) error {
	r, err := c.Checkpoint(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("checkpoint: %v\n", r.Checkpointed)
	return nil
}

func cmdPromote(ctx context.Context, c *client.Client, args []string) error {
	r, err := c.Promote(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("promoted: writes=%v\n", r.Writes)
	return nil
}

func cmdDemote(ctx context.Context, c *client.Client, args []string) error {
	r, err := c.Demote(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("demoted: writes=%v\n", r.Writes)
	return nil
}

func cmdTransfer(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: ost transfer BUCKET URL|MAGNET|FILE.torrent [--key=KEY]")
	}
	key := ""
	for _, a := range args[2:] {
		if strings.HasPrefix(a, "--key=") {
			key = strings.TrimPrefix(a, "--key=")
		}
	}
	src := args[1]
	var t *client.Transfer
	var err error
	switch {
	case strings.HasPrefix(src, "magnet:?"):
		t, err = c.CreateMagnetTransfer(ctx, args[0], key, src)
	case strings.HasSuffix(strings.ToLower(src), ".torrent"):
		data, rerr := os.ReadFile(src)
		if rerr != nil {
			return rerr
		}
		t, err = c.CreateTorrentTransfer(ctx, args[0], key, data)
	default:
		t, err = c.CreateTransfer(ctx, args[0], key, src, "")
	}
	if err != nil {
		return err
	}
	fmt.Printf("queued transfer %s (%s) -> %s/%s (status=%s)\n", t.ID, t.Kind, t.Bucket, t.Key, t.Status)
	return nil
}

func cmdTransfers(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ost transfers BUCKET [cancel|retry ID]")
	}
	if len(args) >= 2 && args[1] == "cancel" {
		if len(args) < 3 {
			return fmt.Errorf("usage: ost transfers BUCKET cancel ID")
		}
		if err := c.CancelTransfer(ctx, args[0], args[2]); err != nil {
			return err
		}
		fmt.Printf("cancelled %s\n", args[2])
		return nil
	}
	if len(args) >= 2 && args[1] == "retry" {
		if len(args) < 3 {
			return fmt.Errorf("usage: ost transfers BUCKET retry ID")
		}
		t, err := c.RetryTransfer(ctx, args[0], args[2])
		if err != nil {
			return err
		}
		fmt.Printf("retrying %s (status=%s, resumed from %s)\n", t.ID, t.Status, human(t.Progress))
		return nil
	}
	list, err := c.ListTransfers(ctx, args[0])
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tKIND\tKEY\tSTATUS\tPROGRESS\tERROR")
	for _, t := range list {
		errTxt := t.Error
		if len(errTxt) > 40 {
			errTxt = errTxt[:40] + "…"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", t.ID, t.Kind, t.Key, t.Status, human(t.Progress), errTxt)
	}
	return w.Flush()
}

func cmdTrash(ctx context.Context, c *client.Client, args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "restore":
			return cmdRestore(ctx, c, args[1:])
		case "purge":
			if len(args) < 3 {
				return fmt.Errorf("usage: ost trash purge BUCKET KEY")
			}
			if err := c.PurgeObject(ctx, args[1], args[2]); err != nil {
				return err
			}
			fmt.Printf("purged %s/%s\n", args[1], args[2])
			return nil
		case "purge-all":
			n, err := c.PurgeAllTrash(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("purged %d objects\n", n)
			return nil
		default:
			return fmt.Errorf("unknown trash subcommand %q", args[0])
		}
	}
	entries, err := c.ListTrash(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET\tKEY\tSIZE\tTRASHED")
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Bucket, e.Key, human(e.Size), e.TrashedAt)
	}
	return w.Flush()
}

func cmdRestore(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: restore BUCKET KEY")
	}
	if err := c.RestoreObject(ctx, args[0], args[1]); err != nil {
		return err
	}
	fmt.Printf("restored %s/%s\n", args[0], args[1])
	return nil
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return strconv.FormatInt(n, 10) + " B"
}
