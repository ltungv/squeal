const std = @import("std");
const testing = std.testing;
const cli = @import("cli.zig");
const squeal = @import("squeal.zig");

pub fn randomTemporaryFilePath(allocator: std.mem.Allocator) ![]u8 {
    const tmpdir = std.testing.tmpDir(.{});
    const tmpdir_path = try tmpdir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpdir_path);

    var random_bytes: [32]u8 = undefined;
    var rng = std.rand.DefaultPrng.init(0);
    rng.random().bytes(&random_bytes);

    var random_subpath: [std.fs.base64_encoder.calcSize(32)]u8 = undefined;
    _ = std.fs.base64_encoder.encode(&random_subpath, &random_bytes);

    return std.fs.path.resolve(allocator, &[_][]const u8{ tmpdir_path, &random_subpath });
}

pub fn expectVmOutputGivenInput(allocator: std.mem.Allocator, path: []const u8, expected: []const u8, input: []const u8) !void {
    const output = try allocator.alloc(u8, expected.len);
    defer allocator.free(output);

    var istream = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(input) };
    var ostream = std.io.StreamSource{ .buffer = std.io.fixedBufferStream(output) };
    const stream = cli.Stream{ .reader = istream.reader(), .writer = ostream.writer() };

    var table = try squeal.Table.init(allocator, path, @sizeOf(squeal.Row), 4096, 512);
    defer table.deinit() catch unreachable;

    var vm = try cli.Vm.init(allocator, &stream, &table);
    try vm.run();
    try std.testing.expectEqualStrings(expected, output);
}
