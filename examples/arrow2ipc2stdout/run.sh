#!/bin/sh

wazero run ./arrow2ipc2stdout.wasm |
	arrow-cat
