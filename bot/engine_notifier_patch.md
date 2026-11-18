Notifier is integrated via engine.set_notifier(callback).
TelegramController calls it on start.
Fills/merges/resolves should call notify through _log_event(..., notify=True) in full engine.
