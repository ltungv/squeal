const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    buildExecutable(b, target, optimize);
    buildUnitTest(b, target, optimize);
}

fn buildExecutable(b: *std.build.Builder, target: std.zig.CrossTarget, optimize: std.builtin.Mode) void {
    const exe = b.addExecutable(.{
        .name = "squeal",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(exe);
    const cmd = b.addRunArtifact(exe);
    cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        cmd.addArgs(args);
    }
    const step = b.step("run", "Run the app");
    step.dependOn(&cmd.step);
}

fn buildUnitTest(b: *std.build.Builder, target: std.zig.CrossTarget, optimize: std.builtin.Mode) void {
    const exe = b.addTest(.{
        .root_source_file = .{ .path = "src/tests.zig" },
        .target = target,
        .optimize = optimize,
    });
    const cmd = b.addRunArtifact(exe);
    const step = b.step("test", "Run unit tests");
    step.dependOn(&cmd.step);
}
