#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: zhipeng
# @Email: zhipeng.py@gmail.com
# @Date:   2020-07-11 00:43:36
# @Last Modified by:   zhipeng
# @Last Modified time: 2020-07-22 21:59:08

from psycopg2.extensions import cursor
from psycopg2 import InterfaceError
from psycopg2.extensions import cursor as _base_cursor
from psycopg2.extensions import connection as _base_connection


class BaseRenew(object):
    """Renew PGSQL Collection or Cursor

    automatic renew connect or cursor when them closed
    :param __fix_attrs__: which attrs should recursive find
    :type __fix_attrs__: list
    :param _sub_obj: recursive new connection or cursor
    :type _sub_obj: postgres connection or cursor
    :param _args: instance args
    :type _args: tuple
    :param _kwargs: instance kwargs
    :type _kwargs: dict
    """
    __fix_attrs__ = []
    _sub_obj = None
    _args = tuple()
    _kwargs = dict()

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @property
    def args(self):
        """self instance args

        renew self use this
        :returns: args
        :rtype: {tuple}
        """
        return super(self.__class__, self).__getattribute__('_args')

    @args.setter
    def args(self, args):
        """set self args


        :param args: self params
        :type args: tuple
        """
        setattr(self, '_args', args)

    @property
    def kwargs(self):
        """self instance kwargs

        renew self use this
        :returns: kwargs
        :rtype: {dict}
        """
        return super(self.__class__, self).__getattribute__('_kwargs')

    @kwargs.setter
    def kwargs(self, kwargs):
        """set self kwargs


        :param kwargs: self params
        :type kwargs: dict
        """
        setattr(self, '_kwargs', kwargs)

    @property
    def sub(self):
        """child instance

        when self closed, will generate child instance.
        if child instance exits, Get attrs from child first.
        :returns: child instance
        :rtype: {psql connection or cursor}
        """
        return super(self.__class__, self).__getattribute__('_sub_obj') or None

    @sub.setter
    def sub(self, obj):
        """set child instance

        connection or cursor instance
        :param obj: child instance
        :type obj: psql connection or cursor
        """
        setattr(self, '_sub_obj', obj)

    def __getattribute__(self, key, *args):
        """Get instance attr

        If key in fix_attrs, and child has been created, get them from child first.
        :param key: key
        :type key: srt
        :param *args: params
        :type *args: tuple
        :returns: child or self attr value
        :rtype: {object}
        """
        if key in super(BaseRenew, self).__getattribute__('__fix_attrs__'):
            if self.sub:
                return getattr(self.sub, key)
        return super(BaseRenew, self).__getattribute__(key, *args)

    def solve_conn_curs(self, cur_conn):
        """renew connection or cursor

        renew connection or cursor
        :param cur_conn: connection or cursor
        :type cur_conn: connection or cursor
        """
        if issubclass(cur_conn.__class__, _base_cursor):
            conn = cur_conn.connection
            if cur_conn.connection.closed:
                # new connection instance
                conn = cur_conn.connection.__class__(*cur_conn.connection.args, **cur_conn.connection.kwargs)
            cur = conn.cursor(*self.args, **self.kwargs)
            self.sub = cur
        elif issubclass(cur_conn.__class__, _base_connection):
            self.sub = cur_conn.__class__(*self.args, **self.kwargs)
        else:
            logging.warning('cant clone child. obj: %s' % cur_conn)

    def close(self, recursive=True):
        """close connect or cursor

        :param recursive: close child instance, defaults to True
        :type recursive: bool, optional
        """
        if recursive and self.sub:
            self.sub.close()
        super(BaseRenew, self).close()

class RenewCursor(_base_cursor, BaseRenew):
    """renew cursor

    automatic renew cursor instance when cursor closed
    :param __fix_attrs__: read attrs from child instance first
    :type __fix_attrs__: list
    """
    __fix_attrs__ = ['closed', 'connection', 'fetchall', 'fetchone', 'fetchall']

    def __init__(self, *args, **kwargs):
        _base_cursor.__init__(self, *args, **kwargs)
        BaseRenew.__init__(self, *args, **kwargs)

    def execute(self, *args, **kwargs):
        """Exec SQL

        execute sql. automatic reconnect when socket closed.
        :param *args: cursor.execute args
        :type *args: tuple
        :param **kwargs: cursor.execute kwargs
        :type **kwargs: dict
        :raises: e
        """
        if self.connection.closed:
            logging.debug("cursor: %s connection: %s closed, renew." % (self, self.connection))
            self.solve_conn_curs(self)
        if self.closed:
            logging.debug("cursor: %s already closed, renew." % self)
            self.solve_conn_curs(self)
        try:
            self.__execute(*args, **kwargs)
        except InterfaceError as e:
            msg = getattr(e, 'message', str(e))
            if "cursor already closed":
                # close current cursor first
                self.close()
                logging.debug("cursor: %s interface err, renew" % self)
                self.solve_conn_curs(self)
            elif "connection already closed" == msg:
                # close current connection first
                self.connection.close()
                logging.debug("cursor: %s connection: %s interface err, renew connection" % (self, self.connection))
                self.solve_conn_curs(self)
            else:
                raise e
            self.__execute(*args, **kwargs)

    def __execute(self, *args, **kwargs):
        """Exec SQL

        use child execute if child instance exists. then try use self.
        :param *args: execute args
        :type *args: tuple
        :param **kwargs: execute kwargs
        :type **kwargs: dict

        """
        if self.sub:
            self.sub.execute(*args, **kwargs)
        else:
            super(RenewCursor, self).execute(*args, **kwargs)


class RetryConnection(_base_connection, BaseRenew):
    """A connection that uses `RenewCursor` automatically."""
    __fix_attrs__ = ['closed', 'connection']

    def __init__(self, *args, **kwargs):
        _base_connection.__init__(self, *args, **kwargs)
        BaseRenew.__init__(self, *args, **kwargs)

    def cursor(self, *args, **kwargs):
        """Overwrite base cursor

        automatic new connection when self closed.
        :param *args: cursor params
        :type *args: tuple
        :param **kwargs: cursor params
        :type **kwargs: dict
        :returns: cursor instance
        :rtype: {cursor}
        """
        cursor_factory = self.cursor_factory or RenewCursor
        if self.closed:
            self.solve_conn_curs(self)
        kwargs.setdefault('cursor_factory', cursor_factory)
        cursor = self.__cursor(*args, **kwargs)
        # required reset args and kwargs !!!
        cursor.args = args
        cursor.kwargs = kwargs
        return cursor

    def __cursor(self, *args, **kwargs):
        """Create Cursor

        use child cursor if child instance exists. then try use self.
        :param *args: cursor args
        :type *args: tuple
        :param **kwargs: cursor kwargs
        :type **kwargs: dict

        """
        if self.sub:
            return self.sub.cursor(*args, **kwargs)
        else:
            return super(RetryConnection, self).cursor(*args, **kwargs)

