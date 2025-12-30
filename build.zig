const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the library module
    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    lib_mod.addIncludePath(b.path("lib/"));

    // Create the static library artifact
    const lib_mod_chdb = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    lib_mod_chdb.addIncludePath(b.path("lib/"));
    lib_mod_chdb.link_libc = true;

    const lib = b.addLibrary(.{
        .name = "chdb_zig",
        .linkage = .static,
        .root_module = lib_mod_chdb,
    });

    b.installArtifact(lib);

    // Integration tests configuration
    const connection_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/connection_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Configure module imports for tests
    const conn_test_module = connection_tests.root_module;
    conn_test_module.addImport("chdb_zig_lib", lib_mod);
    conn_test_module.addIncludePath(b.path("lib/"));
    conn_test_module.link_libc = true;

    // Link against libchdb.so
    connection_tests.addLibraryPath(b.path("lib"));
    connection_tests.linkSystemLibrary("chdb");

    // Run step for tests
    const run_connection_tests = b.addRunArtifact(connection_tests);

    // Ensure libchdb.so can be found at runtime

    // Custom test step
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_connection_tests.step);
}
