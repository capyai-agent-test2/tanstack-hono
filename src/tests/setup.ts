const refreshNoop = () => {};

Object.assign(globalThis, {
	$RefreshReg$: refreshNoop,
	$RefreshSig$: () => refreshNoop,
});
