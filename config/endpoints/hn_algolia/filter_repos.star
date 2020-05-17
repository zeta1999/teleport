def transform(objects):
  newObjects = []
  for object in objects:
    print(object['url'])
    if object['url'].rfind("github.com/blog/") != -1:
      continue
    if object['url'].rfind("github.com/features/") != -1:
      continue
    if object['url'].rfind("blog.github.com/") != -1:
      continue

    newObjects.append(object)

  return newObjects
