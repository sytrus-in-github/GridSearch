# -*- coding: utf-8 -*-
"""
Created on Thu May 11 11:55:37 2017

@author: Yuesong Shen

this script provide grid search utility which can be persistent by saving a 
backup session after each run. 
"""
from abc import ABCMeta, abstractmethod
try:
    import cPickle as pickle
except:
    import pickle
import datetime


def gridSearch(
        func=None, 
        posparamlists=(), 
        kwparamlists=frozenset(dict().items()),
        comparer=None, 
        logger=None,
        backupSession=None,
        loadSession=None):
    """grid search for parameters of the function func
    func:           the function to do grid search with
    posparamlists:  positional argument lists to test with (list of list)
    kwarglists:     keyword argument lists to test with (dict of list)
    comparer:       instance of Comparer, if specified, will compare output of func to find the best one
    logger:         instance of Logger, if specified, will customize log for results. otherwise a default logger will be used
    backupSession:  path to store session pickle, if specified will pickle session after each run
    loadsession:    path to existing session pickle, if specified will continue the specified session and ignore other parameters
    """
    if loadSession is not None:
        with open(loadSession,'rb') as fileContent:
            session = pickle.load(fileContent)
    else:
        session = GridSearchSession(func, posparamlists, kwparamlists, comparer, logger)
        session.initialize()
    if backupSession is None:
        session.spin()
    else:
        while session.isRunning():
            session.spinOnce()
            with open(backupSession,'wb') as f:
                pickle.dump(session, f)


class GridSearchSession:
    """a session of grid search containing the state of the system"""
    def __init__(
            self, 
            func, 
            posparamlists=(), 
            kwparamlists=frozenset(dict().items()),
            comparer=None, 
            logger=None):
        self._isrunning = False
        self.func = func
        self.posargs = posparamlists
        self.kwargs = dict(kwparamlists)
        self.kwnames = list(self.kwargs.keys())
        self.comparer = comparer
        if logger is None: self.logger = DefaultLogger()
        else: self.logger = logger
        if len(self.posargs) ==0 and len(self.kwnames) == 0:
            raise ValueError('no parameter to iterate with.')
        elif len(self.posargs) == 0:
            self.iterator = ConstantExtendedRangeIterator(CombinedRangeIterator([IntRangeIterator(len(self.kwargs[k])) for k in self.kwnames]), [], None)
        elif len(self.kwnames) == 0:
            self.iterator = ConstantExtendedRangeIterator(CombinedRangeIterator([IntRangeIterator(len(pl)) for pl in self.posargs]), None, [])
        else:
            self.iterator = CombinedRangeIterator(
                [CombinedRangeIterator([IntRangeIterator(len(pl)) for pl in self.posargs]),
                 CombinedRangeIterator([IntRangeIterator(len(self.kwargs[k])) for k in self.kwnames])])
        self.bestOutput = None
        self.bestParams = ([], {})
        
    def initialize(self):
        self._isrunning = True
        self.logger.initialize()
        self.iterator.reset()
        
    def isRunning(self):
        return self._isrunning
        
    def _getPosParam(self, indices):
        posparams = self.posargs
        return [posparams[i][indices[i]] for i in range(len(posparams))]
        
    def _getKwParam(self, indices):
        param = dict()
        kwparams = self.kwargs
        for (i,k) in enumerate(self.kwnames):
            param[k] = kwparams[k][indices[i]]
        return param

    def _notifyComparer(self, posparam, kwparam, output):        
        if self.comparer is not None:
            if self.bestOutput is None or \
               self.comparer.leftBetterThanRight(output, self.bestOutput):
                self.bestParams = (posparam, kwparam)
                self.bestOutput = output
    
    def _notifyLogger(self, posparam, kwparam, output):
        self.logger.update(posparam, kwparam, output)
    
    def _finalizeLogger(self):
        bestposparams, bestkwparams = self.bestParams
        self.logger.logBest(bestposparams, bestkwparams, self.bestOutput)
        
    def spinOnce(self):
        if not self._isrunning:
            raise Exception('Session is not running. Initialization needed.')
        indices = self.iterator.iterate()
        if indices is not None:
            posparam = self._getPosParam(indices[0])
            kwparam = self._getKwParam(indices[1])
            output = self.func(*posparam, **kwparam)
            self._notifyComparer(posparam, kwparam, output)
            self._notifyLogger(posparam, kwparam, output)
        else:
            self._isrunning = False
            self._finalizeLogger()
    
    def spin(self):
        while self.isRunning():
            self.spinOnce()


class RangeIterator:
    """interface of RangeIterator, suppose can iterate at least once"""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def iterate(self):
        raise NotImplementedError
    
    @abstractmethod
    def hasNext(self):
        raise NotImplementedError

    @abstractmethod
    def get(self):
        raise NotImplementedError

    @abstractmethod
    def reset(self):
        raise NotImplementedError


class IntRangeIterator(RangeIterator):
    def __init__(self, upperbound):
        self.upperbound = upperbound
        self.pointer = 0
        
    def iterate(self):
        g = self.get()
        if g is not None:
            self.pointer += 1
        return g
    
    def get(self):
        if not self.hasNext():
            return None
        else:
            return self.pointer
    
    def hasNext(self):
        return self.pointer < self.upperbound

    def reset(self):
        self.pointer = 0


class CombinedRangeIterator(RangeIterator):
    def __init__(self, rangeIterators):
        self.rangeiters = rangeIterators
        self.reset()
        
    def iterate(self):
        g = self.get()
        if g is not None:
            pt = 0
            ris = self.rangeiters
            while pt < len(ris):
                ris[pt].iterate()
                if ris[pt].get() is None:
                    ris[pt].reset()
                    pt += 1
                else:
                    return g
        self.iterable = False
        return g
        
    def hasNext(self):
        return self.iterable

    def get(self):
        if not self.hasNext():
            return None
        else:
            return [ri.get() for ri in self.rangeiters]
        
    def reset(self):
        for ri in self.rangeiters:
            ri.reset()
        self.iterable = self.rangeiters and all([ri.hasNext() for ri in self.rangeiters])


class ConstantExtendedRangeIterator(RangeIterator):
    def __init__(self, rangeiterator, constListBefore, constListAfter):
        self.iterator = rangeiterator
        self.lconst = constListBefore
        self.rconst = constListAfter
        self.reset()
        
    def iterate(self):
        g = self.get()
        if g is not None:
            self.iterator.iterate()
        return g
    
    def hasNext(self):
        return self.iterator.hasNext()

    def get(self):
        if not self.hasNext():
            return None
        else:
            return (tuple(self.lconst) if self.lconst is not None else ()) + \
                   (self.iterator.get(),) + \
                   (tuple(self.rconst) if self.rconst is not None else ())

    def reset(self):
        return self.iterator.reset()


class Logger:
    """interface of logger to use for gridSearch"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def initialize(self):
        raise NotImplementedError
    
    @abstractmethod
    def update(self, posparams, kwparams, output):
        raise NotImplementedError
        
    @abstractmethod
    def logBest(self, bestposparams, bestkwparams, bestoutput):
        raise NotImplementedError


class DefaultLogger(Logger):
    def __init__(self, name = 'Default Logger'):
        self.results = dict()
        self.best = None
        self.name = str(name)

    def initialize(self):
        self.results = dict()
        self.best = None
        print '\n'+self.name, 'starts at', '{:%Y-%m-%d %H:%M:%S}\n\n'.format(datetime.datetime.now())

    def update(self, posparams, kwparams, output):
        params = tuple(posparams)+tuple(kwparams.items())
        self.results[params] = output
        print params, ':', output
        
    def logBest(self, bestposparams, bestkwparams, bestoutput):
        if bestoutput is not None:
            bestparams = tuple(bestposparams)+tuple(bestkwparams.items())
            self.best = (bestparams , bestoutput)
            print '\nBest:\n', bestparams, ':' , bestoutput


class PersistentLogger(Logger):
    def __init__(self, logfile, name='Persistent Logger'):
        self.results = dict()
        self.best = None
        self.name = str(name)
        self.logfile = logfile 
    
    def initialize(self):
        self.results = dict()
        self.best = None
        head = '\n'+self.name+' starts at {:%Y-%m-%d %H:%M:%S}\n'.format(datetime.datetime.now())
        print head        
        with open(self.logfile,'a') as logfile:
            logfile.write(head+'\n')
        
    def update(self, posparams, kwparams, output):
        params = tuple(posparams)+tuple(kwparams.items())
        self.results[params] = output
        print params, ':', output
        with open(self.logfile,'a') as logfile:
            logfile.write(str(params)+' : '+str(output)+'\n')
        
    def logBest(self, bestposparams, bestkwparams, bestoutput):
        if bestoutput is not None:
            bestparams = tuple(bestposparams)+tuple(bestkwparams.items())
            self.best = (bestparams , bestoutput)
            print '\nBest:\n', bestparams, ':' , bestoutput
            with open(self.logfile,'a') as logfile:
                logfile.write('\nBest:\n'+str(bestparams)+' : '+str(bestoutput)+'\n')


class Comparer:
    """interface of comparer to use for gridSearch"""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def leftBetterThanRight(self, left, right):
        raise NotImplementedError


class DefaultComparer:
    def leftBetterThanRight(self, left, right):
        return left > right
