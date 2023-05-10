class EventEmitter:
  def __init__(self):
    self._listeners = {}

  def on(self, event, listener):
    if event not in self._listeners:
      self._listeners[event] = []
    self._listeners[event].append(listener)

  def off(self, event, listener):
    if event in self._listeners:
      self._listeners[event].remove(listener)

  def emit(self, event, *args, **kwargs):
    if event in self._listeners:
      for listener in self._listeners[event]:
        listener(*args, **kwargs)
