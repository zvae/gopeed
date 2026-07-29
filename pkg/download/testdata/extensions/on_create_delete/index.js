gopeed.events.onCreate(function (ctx) {
    // exercise the file API the same way real skip-exists extensions do,
    // a broken gopeed.file injection must fail this handler (and the test)
    var exists = gopeed.file.exists(ctx.task.meta.opts.path + "/no-such-file");
    gopeed.logger.info('delete task on create', ctx.task.id, 'exists:', exists);
    ctx.task.delete(false);
});
