gopeed.events.onCreate(function (ctx) {
    gopeed.logger.info('delete task on create', ctx.task.id);
    ctx.task.delete(false);
});
