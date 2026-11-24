//go:build js && wasm

// Command wasm exposes the demo simulation to the page as window.vigil.
// It only converts between JavaScript values and demo/sim: no logic here.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/kabilaymen/vigil/demo/sim"
)

func main() {
	s := sim.New(sim.DefaultConfig(1))
	fn := func(f func(a []js.Value) any) js.Func {
		return js.FuncOf(func(_ js.Value, a []js.Value) any { return f(a) })
	}
	js.Global().Set("vigil", js.ValueOf(map[string]any{
		"step": fn(func(a []js.Value) any { s.Step(a[0].Int()); return nil }),
		"state": fn(func([]js.Value) any {
			b, _ := json.Marshal(s.State())
			return string(b)
		}),
		"phi": fn(func(a []js.Value) any {
			b, _ := json.Marshal(s.PhiHistory(a[0].String()))
			return string(b)
		}),
		"crash":    fn(func(a []js.Value) any { s.Crash(a[0].String()); return nil }),
		"stall":    fn(func(a []js.Value) any { s.Stall(a[0].String()); return nil }),
		"setChaos": fn(func(a []js.Value) any { s.SetChaos(a[0].Bool()); return nil }),
		"reset": fn(func(a []js.Value) any {
			s = sim.New(sim.DefaultConfig(int64(a[0].Int())))
			return nil
		}),
	}))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("vigil-ready"))
	select {}
}
