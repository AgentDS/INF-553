[TOC]

# Map-Reduce Examples

### 1. Word length histogram

__Map task:__


__Reduce task:__



### 2. Host size

__Map task:__ for each record, output hostname (URL, size)

__Reduce task:__ Sum the sizes for each host



### 3. Language Model

__Map task:__ Extract (5-word sequence, count) from document

__Reduce task:__ combine the counts

