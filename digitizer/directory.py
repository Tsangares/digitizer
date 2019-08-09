from os import listdir,path
def findFile(folder,condition):
    files=[]
    contents=listdir(folder)
    for content in contents:
        contentPath=path.join(folder,content)
        if path.isdir(contentPath):
            files+=findFile(contentPath,condition)
        elif condition(content):
            files.append(contentPath)
    return files
