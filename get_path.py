import os
import glob
from pathlib import Path
import re
from typing import Generator, List
from func_helper import pip, tee, identity
import iter_helper as it

"""
与えられた正規表現パターン `patterns` に対し,
パターンにマッチするパスのみを返す.
デフォルトではあらゆるパスにマッチする.

指定したディレクトリ `dirPath` 以下を再帰的に探索し,
ディレクトリとファイルパスの一覧を取得する.
"""


def getAllSubPath(_directory):
    directory = _directory if re.search(
        "/$", _directory) != None else _directory + "/"
    return glob.glob(directory+"**", recursive=True)


def isMatchAll(patterns):
    return lambda s: it.reducing(lambda a, b: a and b)(True)(
        it.mapping(lambda pattern: re.search(pattern, s) != None)(patterns)
    )


class PathList:
    def __init__(self, pathList):
        self.paths = list(pathList)

    def __add__(self, pathlist):
        return PathList([*self.paths, *pathlist.paths])

    def __len__(self):
        return len(self.paths)

    def directories(self, verbose=False):
        return pip(
            it.filtering(os.path.isdir),
            list,
            tee(
                pip(
                    enumerate,
                    list,
                    print if verbose else identity
                )
            )
        )(self.paths)

    def files(self, verbose=False):
        return pip(
            it.filtering(os.path.isfile),
            list,
            tee(
                pip(
                    enumerate,
                    list,
                    print if verbose else identity
                )
            )
        )(self.paths)

    @staticmethod
    def match(*patterns: List[str]):
        def directory_from(*search_roots: str):
            map_get_paths = it.mapping(pip(
                getAllSubPath,
                it.filtering(isMatchAll(patterns)),
            ))

            def concat(acc, e):
                return [*acc, *e]
            return PathList(it.reducing(concat)([])(map_get_paths(search_roots)))
        return directory_from

    @staticmethod
    def search(*patterns):
        print("PathList.search is deplicated. Use PathList.match")

        def directory_from(*roots):
            map_get_paths = it.mapping(pip(
                getAllSubPath,
                it.filtering(isMatchAll(patterns)),
            ))

            def concat(acc, e):
                return [*acc, *e]
            return PathList(it.reducing(concat)([])(map_get_paths(roots)))
        return directory_from

    @staticmethod
    def to_strings(pathLike):
        if type(pathLike) is PathList:
            return pathLike.files()
        elif type(pathLike) in [list, tuple]:
            return sum([PathList.to_strings(path) for path in pathLike], [])
        elif type(pathLike) is str:
            return [pathLike]
        elif isinstance(pathLike, Path):
            return [str(pathLike.resolve())]
        else:
            print(pathLike)
            raise TypeError("Invalid data source type.")


def getFileList(*patterns):
    return lambda dirPath: pip(
        getAllSubPath,
        it.filtering(isMatchAll(patterns)),
        list,
        PathList
    )(dirPath)


def multiple_path_finder(*patterns):
    def apply(roots):
        if type(roots) is str:
            return [getFileList(*patterns)(roots)]
        else:
            return [getFileList(*patterns)(root) for root in roots]
    return apply
