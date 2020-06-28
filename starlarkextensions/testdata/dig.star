load('assert.star', 'assert')

dict = { "a": { "b": [{ "c": 4 }] } }
list = [{"a": 9}]
dbllist = [["a"]]

assert.eq({ "a": { "b": [{ "c": 4 }] } }, dig(dict))
assert.eq({ "b": [{ "c": 4 }] }, dig(dict, "a"))
assert.eq([{ "c": 4 }], dig(dict, "a", "b"))
assert.eq({ "c": 4 }, dig(dict, "a", "b", 0))
assert.eq(4, dig(dict, "a", "b", 0, "c"))

assert.eq([{ "a" : 9 }], dig(list))
assert.eq({ "a" : 9 }, dig(list, 0))
assert.eq(9, dig(list, 0, "a"))

assert.eq([["a"]], dig(dbllist))
assert.eq(["a"], dig(dbllist, 0))
assert.eq("a", dig(dbllist, 0, 0))

assert.eq(None, dig(dict, "a", "b", 1, "c"))
assert.eq(None, dig(dict, "a", "b", 1, "c", "d"))
assert.eq(None, dig(dict, 0, "a", "b"))
assert.eq(None, dig(list, 1))
assert.eq(None, dig(dbllist, 0, 1))

