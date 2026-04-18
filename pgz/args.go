package pgz

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/arturoeanton/pgz/internal/protocol"
	"github.com/arturoeanton/pgz/internal/wire"
)

// pgEpoch is 2000-01-01 00:00:00 UTC, the base for PG timestamp wire
// values (POSTGRES_EPOCH_JDATE in PG's adt/timestamp.c).
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// parseParamOIDs decodes a ParameterDescription body: int16 count +
// int32 oid * count. Returns nil when the count is zero (parameterless
// prepared statement).
func parseParamOIDs(body []byte) []protocol.OID {
	if len(body) < 2 {
		return nil
	}
	n := int(int16(binary.BigEndian.Uint16(body[0:2])))
	if n <= 0 || len(body) < 2+4*n {
		return nil
	}
	out := make([]protocol.OID, n)
	for i := 0; i < n; i++ {
		out[i] = protocol.OID(binary.BigEndian.Uint32(body[2+4*i : 2+4*i+4]))
	}
	return out
}

// writeBindParams writes the Bind-message parameter block: per-column
// format codes (count + codes) followed by values (count + value). It
// chooses binary format for scalar types the server can decode in
// binary and text for everything else (strings, numerics, unknown
// OIDs). Binary encoding is zero-alloc because values are written
// directly into the wire builder's reusable buffer.
//
// oids comes from the server's ParameterDescription (see
// parseParamOIDs). An empty or all-zero slice means the server could
// not infer the parameter types; in that case every parameter falls
// back to text encoding — the existing behaviour.
func writeBindParams(bd *wire.Builder, oids []protocol.OID, args []any) error {
	if len(args) == 0 {
		bd.Int16(0) // 0 parameter format codes
		bd.Int16(0) // 0 parameters
		return nil
	}

	// Decide per-parameter format. paramBinary returns true when both
	// the server OID and the Go value support binary encoding.
	bd.Int16(int16(len(args)))
	for i, a := range args {
		var oid protocol.OID
		if i < len(oids) {
			oid = oids[i]
		}
		if paramBinary(oid, a) {
			bd.Int16(1)
		} else {
			bd.Int16(0)
		}
	}

	// Values.
	bd.Int16(int16(len(args)))
	for i, a := range args {
		var oid protocol.OID
		if i < len(oids) {
			oid = oids[i]
		}
		if a == nil {
			bd.Int32(-1)
			continue
		}
		if paramBinary(oid, a) {
			if err := appendParamBinary(bd, oid, a); err != nil {
				return err
			}
			continue
		}
		if err := appendParamText(bd, a); err != nil {
			return err
		}
	}
	return nil
}

// paramBinary reports whether (oid, v) can be encoded in binary wire
// format. We require a concrete server OID because the encoding width
// depends on it (int2 vs int4 vs int8 all arrive as Go int64 from
// database/sql). Unknown OID → fall back to text so the server can
// infer.
func paramBinary(oid protocol.OID, v any) bool {
	if v == nil {
		return false
	}
	switch oid {
	case protocol.OIDBool:
		_, ok := v.(bool)
		return ok
	case protocol.OIDInt2, protocol.OIDInt4, protocol.OIDInt8, protocol.OIDOID:
		switch v.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case protocol.OIDFloat4, protocol.OIDFloat8:
		switch v.(type) {
		case float32, float64:
			return true
		}
		return false
	case protocol.OIDBytea:
		_, ok := v.([]byte)
		return ok
	case protocol.OIDTimestamp, protocol.OIDTimestampTZ:
		_, ok := v.(time.Time)
		return ok
	case protocol.OIDText, protocol.OIDVarchar, protocol.OIDBPChar, protocol.OIDName:
		// Text types: binary format is the same bytes as text, no
		// speedup — keep text so stringification matches existing
		// tests that inspect the wire.
		return false
	}
	return false
}

// appendParamBinary writes length + raw binary bytes for v given the
// server-chosen oid. Must only be called when paramBinary(oid, v)
// returned true.
func appendParamBinary(bd *wire.Builder, oid protocol.OID, v any) error {
	switch oid {
	case protocol.OIDBool:
		bd.Int32(1)
		if v.(bool) {
			bd.Byte(1)
		} else {
			bd.Byte(0)
		}
		return nil
	case protocol.OIDInt2:
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		bd.Int32(2)
		bd.Int16(int16(n))
		return nil
	case protocol.OIDInt4, protocol.OIDOID:
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		bd.Int32(4)
		bd.Int32(int32(n))
		return nil
	case protocol.OIDInt8:
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		bd.Int32(8)
		bd.Int32(int32(n >> 32))
		bd.Int32(int32(n))
		return nil
	case protocol.OIDFloat4:
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		bd.Int32(4)
		bd.Int32(int32(math.Float32bits(float32(f))))
		return nil
	case protocol.OIDFloat8:
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		u := math.Float64bits(f)
		bd.Int32(8)
		bd.Int32(int32(u >> 32))
		bd.Int32(int32(u))
		return nil
	case protocol.OIDBytea:
		b := v.([]byte)
		bd.Int32(int32(len(b)))
		bd.Bytes(b)
		return nil
	case protocol.OIDTimestamp, protocol.OIDTimestampTZ:
		t := v.(time.Time)
		us := t.UTC().Sub(pgEpoch).Microseconds()
		bd.Int32(8)
		bd.Int32(int32(us >> 32))
		bd.Int32(int32(us))
		return nil
	}
	return fmt.Errorf("pgz: appendParamBinary: unhandled OID %d", oid)
}

// appendParamText is the legacy text-encoding path. Still used for
// strings, numeric (where server text is faster), unknown OIDs, and
// anything paramBinary rejected.
func appendParamText(bd *wire.Builder, v any) error {
	s, isNull, err := encodeArg(v)
	if err != nil {
		return err
	}
	if isNull {
		bd.Int32(-1)
		return nil
	}
	bd.Int32(int32(len(s)))
	bd.String(s)
	return nil
}

// toInt64 promotes any Go integer-ish value to int64. Used by the
// binary integer encoders to accept the common Go types without
// duplicating each case in the type-switch.
func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	}
	return 0, fmt.Errorf("pgz: cannot convert %T to int64", v)
}

// toFloat64 promotes float32/float64 to float64.
func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	}
	return 0, fmt.Errorf("pgz: cannot convert %T to float64", v)
}

// encodeArg renders a parameter as the text wire form. Returns
// (value, isNull, error). Called by appendParamText and by
// callers that always want text (internal COPY paths, etc.).
func encodeArg(v any) (string, bool, error) {
	switch x := v.(type) {
	case nil:
		return "", true, nil
	case string:
		return x, false, nil
	case []byte:
		// Render as bytea hex literal: "\\x..."
		const hex = "0123456789abcdef"
		buf := make([]byte, 2+2*len(x))
		buf[0] = '\\'
		buf[1] = 'x'
		for i, b := range x {
			buf[2+2*i] = hex[b>>4]
			buf[2+2*i+1] = hex[b&0xF]
		}
		return string(buf), false, nil
	case bool:
		if x {
			return "t", false, nil
		}
		return "f", false, nil
	case int:
		return strconv.FormatInt(int64(x), 10), false, nil
	case int32:
		return strconv.FormatInt(int64(x), 10), false, nil
	case int64:
		return strconv.FormatInt(x, 10), false, nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), false, nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), false, nil
	case uint64:
		return strconv.FormatUint(x, 10), false, nil
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), false, nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), false, nil
	case time.Time:
		// ISO 8601 with microseconds; PG accepts it in text mode.
		return x.UTC().Format("2006-01-02 15:04:05.999999-07"), false, nil
	default:
		return "", false, fmt.Errorf("pgz: unsupported parameter type %T", v)
	}
}
