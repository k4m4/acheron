def capture(*args1):
  def decorator(fn):
    async def wrapper(*args2, **kwargs2):
      return await fn(*[*args1, *args2], **kwargs2)
    return wrapper
  return decorator
