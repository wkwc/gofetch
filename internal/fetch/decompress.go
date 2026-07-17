package fetch

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// DecompressReader wraps an io.ReadCloser with decompression based on encoding
func DecompressReader(r io.ReadCloser, encoding string) (io.ReadCloser, error) {
	switch encoding {
	case "gzip", "x-gzip":
		gr, err := gzip.NewReader(nil)
		if err != nil {
			return nil, err
		}
		return &decompressReaderCloser{Reader: gr, closer: r}, nil

	case "zstd":
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		return &decompressReaderCloser{Reader: dec, closer: r}, nil

	case "xz", "lzma":
		xzr, err := xz.NewReader(nil)
		if err != nil {
			return nil, err
		}
		return &xzReadCloser{Reader: xzr, closer: r}, nil

	default:
		return r, nil
	}
}

type decompressReaderCloser struct {
	io.Reader
	closer io.Closer
}

func (d *decompressReaderCloser) Close() error {
	if d.closer != nil {
		return d.closer.Close()
	}
	return nil
}

// xzReadCloser wraps xz.Reader to implement io.ReadCloser
type xzReadCloser struct {
	*xz.Reader
	closer io.Closer
}

func (x *xzReadCloser) Close() error {
	if x.closer != nil {
		return x.closer.Close()
	}
	return nil
}

// DecompressToFile decompresses a stream to a file writer
func DecompressToFile(r io.Reader, w io.Writer, encoding string) (int64, error) {
	switch {
	case isGzip(encoding):
		gr, err := gzip.NewReader(nil)
		if err != nil {
			return 0, err
		}
		defer gr.Close()
		return io.Copy(w, gr)

	case isZstd(encoding):
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return 0, err
		}
		defer dec.Close()
		dec.Reset(r)
		return io.Copy(w, dec)

	case isXz(encoding):
		xzr, err := xz.NewReader(r)
		if err != nil {
			return 0, err
		}
		return io.Copy(w, xzr)

	default:
		return io.Copy(w, r)
	}
}

func isGzip(e string) bool { return e == "gzip" || e == "x-gzip" }
func isZstd(e string) bool { return e == "zstd" }
func isXz(e string) bool   { return e == "xz" || e == "lzma" }
