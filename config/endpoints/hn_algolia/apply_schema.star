def transform(objects):
  newObjects = []
  for object in objects:
    newObjects.append({
      'id': object['objectID'],
      'points': object['points'],
      'num_comments': object['num_comments'],
      'url': object['url'],
      'title': object['title'],
      'author': object['author']
    })

  return newObjects
