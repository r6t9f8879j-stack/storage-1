// torlab is a dev-only helper for exercising torrent transfers without any
// external network: it generates valid .torrent files and can serve the data
// over HTTP as a BEP 19 web seed.
//
//	torlab gen <root> <out.torrent> <baseURL>   # also prints an equivalent magnet
//	torlab serve <root> <port>
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: torlab gen|serve ...")
	}
	switch os.Args[1] {
	case "gen":
		fs := flag.NewFlagSet("gen", flag.ExitOnError)
		fs.Parse(os.Args[2:])
		if fs.NArg() != 3 {
			log.Fatal("usage: torlab gen <root> <out.torrent> <baseURL://dir>")
		}
		root, out, base := fs.Arg(0), fs.Arg(1), fs.Arg(2)
		gen(root, out, base)
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ExitOnError)
		fs.Parse(os.Args[2:])
		if fs.NArg() != 2 {
			log.Fatal("usage: torlab serve <root> <:port>")
		}
		root, addr := fs.Arg(0), fs.Arg(1)
		http.Handle("/", http.FileServer(http.Dir(root)))
		log.Printf("serving %s on %s", root, addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	default:
		log.Fatalf("unknown subcommand %q", os.Args[1])
	}
}

func gen(root, out, base string) {
	info := metainfo.Info{}
	if err := info.BuildFromFilePath(root); err != nil {
		log.Fatalf("build info: %v", err)
	}
	b4, err := bencode.Marshal(&info)
	if err != nil {
		log.Fatalf("encode info: %v", err)
	}
	mi := metainfo.MetaInfo{InfoBytes: bencode.Bytes(b4)}
	mi.UrlList = metainfo.UrlList{base}
	f, err := os.Create(out)
	if err != nil {
		log.Fatalf("create %s: %v", out, err)
	}
	if err := mi.Write(f); err != nil {
		log.Fatalf("write torrent: %v", err)
	}
	_ = f.Close()
	fmt.Printf("wrote %s (%d bytes)\n", out, len(b4))

	h := mi.HashInfoBytes()
	m := mi.Magnet(&h, &info)
	m.Params.Set("ws", base)
	m.Params.Set("dn", info.BestName())
	fmt.Printf("magnet: %s\n", m.String())
}
