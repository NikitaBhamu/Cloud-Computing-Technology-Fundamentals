from abc import ABC
import json
import re
import redis
from typing import List,Tuple


curr_ip="10.17.5.87"

class Lab3Redis(ABC):
  """
  __init__ accepts a list of IP addresses on which redis is deployed.
  Number of IPs is typically 3.
  """

  def __init__(self, ips: List[str]):
    self.conns = [redis.Redis(host=ip, decode_responses=True, socket_timeout=5) for ip in ips]
    self.num_instances = len(ips)
    self.index=ips.index(curr_ip)

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    pass


class ConsistentRedis(Lab3Redis):

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    pass


class AvailableRedis(Lab3Redis):
  """
  This method is necessary for evaluation
  """

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    
#if repair, then first repair and then
    if repair:
      diff_sets = [rds.smembers("FILES") for rds in self.conns]
      i=0
      while (i<len(diff_sets)):
        j=0
        while (j<len(diff_sets)):
          diff = diff_sets[i] - diff_sets[j]
          if len(diff) == 0:
            j+=1
            continue
          self.conns[j].sadd("FILES", *diff)
          j+=1
        i+=1

      nodes = self.conns[self.index].smembers("FILES")
      
      wc = {}
      for node in nodes:
        for word, count in json.loads(list(json.loads(node).values())[0]).items():
          if word not in wc:
            wc[word] = 0
          wc[word] += count
      return sorted(wc.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:n]
    
    else:
      nodes = self.conns[self.index].smembers("FILES")
    
      wc = {}
      for node in nodes:
        for word, count in json.loads(list(json.loads(node).values())[0]).items():
          if word not in wc:
            wc[word] = 0
          wc[word] += count
      return sorted(wc.items(), key=lambda x: (x[1],x[0]), reverse=True)[0:n]

  
