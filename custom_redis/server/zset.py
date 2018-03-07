# -*- coding:utf-8 -*-
from .errors import Empty
from bisect import bisect_left, bisect_right, insort


class SNode:
    """
    节点
    """
    def __init__(self, key=None, score=float('-inf'), next=None):
        self.key = key
        self.score = score

    def __lt__(self, other):
        return self.score < getattr(other, 'score', other)

    def __gt__(self, other):
        """
        没定义__gt__的话会导致bisect_right出问题,即使已经定义了__lt__
        :param other:
        :return:
        """
        return self.score > getattr(other, 'score', other)


class SList(object):
    """
    定义数组,用bisect维护顺序
    """
    def __init__(self):
        self.key2node = {}
        self.card = 0
        self.orderlist = []

    def findpos(self, snode):
        curpos = bisect_left(self.orderlist, snode)
        while 1:
            if self.orderlist[curpos].key == snode.key:
                break
            curpos += 1
        return curpos

    def insert(self, key, score):
        if not isinstance(score, int):
            raise Exception('score must be integer')
        snode = self.key2node.get(key)
        if snode:
            if score == snode.score:
                return 0
            del self.orderlist[self.findpos(snode)]
            snode.score = score
        else:
            self.card += 1
            snode = SNode(key=key, score=score)
            self.key2node[key] = snode
        insort(self.orderlist, snode)
        return 1

    def delete(self, key):
        snode = self.key2node.get(key)
        if not snode:
            return 0
        self.card -= 1
        del self.orderlist[self.findpos(snode)]
        del self.key2node[key]
        del snode
        return 1

    def search(self, key):
        return self.key2node.get(key)


class SortedSet:
    def __init__(self):
        self.slist = SList()

    def zadd(self, key, score):
        return self.slist.insert(key, score)

    def zrem(self, key):
        return self.slist.delete(key)

    def zrank(self, key):#score相同则按字典序
        snode = self.slist.key2node.get(key)
        if not snode:
            return None
        return self.slist.findpos(snode)

    def zrevrank(self, key):
        return self.zcard - 1 - self.zrank(key)

    def zscore(self, key):
        snode = self.slist.key2node.get(key)
        return getattr(snode, 'score', None)

    def zcount(self, start, end):
        ol = self.slist.orderlist
        return bisect_left(ol, end+1) - bisect_right(ol, start-1)

    @property
    def zcard(self):
        return self.slist.card

    def zrange(self, start, end, withscores=False):#score相同则按字典序
        nodes = self.slist.orderlist[start: end+1]
        if not nodes:
            return []
        if withscores:
            return [(x.key, x.score) for x in nodes]
        else:
            return [x.key for x in nodes]

    def zrevrange(self, start, end, withscores=False):
        card = self.zcard
        if end<0:
            end = end + card
        if start<0:
            start = start + card
        nodes = self.slist.orderlist[max(card-end-1, 0): max(card-start, 0)][::-1]
        if not nodes:
            return []
        if withscores:
            return [(x.key, x.score) for x in nodes]
        else:
            return [x.key for x in nodes]

    def zrangebyscore(self, start, end, withscores=False):
        ol = self.slist.orderlist
        nodes = ol[bisect_left(ol, start):bisect_right(ol, end)]
        if not nodes:
            return []
        if withscores:
            return [(x.key, x.score) for x in nodes]
        else:
            return [x.key for x in nodes]

    def zpop(self, withscores=False):
        data = self.zrange(0, 0, withscores)
        if data:
            data = data[0]
            if withscores:
                self.zrem(data[0])
            else:
                self.zrem(data)
            return data
        raise Empty

    def zrevrangebyscore(self, end, start, withscores=False):
        return self.zrangebyscore(start, end, withscores)[::-1]

    def zincrby(self, key):
        snode = self.slist.key2node.get(key)
        if not snode:
            return self.zadd(key, 1)
        score = snode.score
        self.zrem(key)
        return self.zadd(key, score+1)

    def __len__(self):
        return self.zcard


if __name__ == "__main__":
    a = SortedSet()