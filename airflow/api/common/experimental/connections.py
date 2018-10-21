from sqlalchemy.orm import exc
from airflow import settings, LoggingMixin
from airflow.exceptions import MissingArgument, ConnectionNotFound, MultipleConnectionsFound
from airflow.models import Connection


def add_connection(
    conn_id,
    conn_uri=None,
    conn_type=None,
    conn_host=None,
    conn_login=None,
    conn_password=None,
    conn_schema=None,
    conn_port=None,
    conn_extra=None,
):
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = list()
    invalid_args = list()
    if not conn_id:
        missing_args.append('conn_id')

    if conn_uri:
        if conn_type:
            invalid_args.append(conn_type)
        if conn_host:
            invalid_args.append(conn_host)
        if conn_login:
            invalid_args.append(conn_login)
        if conn_password:
            invalid_args.append(conn_password)
        if conn_schema:
            invalid_args.append(conn_schema)
        if conn_port:
            invalid_args.append(conn_port)
    elif not conn_type:
        missing_args.append('conn_uri or conn_type')

    if missing_args:
        msg = ('\n\tThe following args are required to add a connection:' +
               ' {missing!r}\n'.format(missing=missing_args))
        raise MissingArgument(msg)
    if invalid_args:
        msg = ('\n\tThe following args are not compatible with the ' +
               '--add flag and --conn_uri flag: {invalid!r}\n')
        msg = msg.format(invalid=invalid_args)
        raise MissingArgument(msg)
    if missing_args or invalid_args:
        return
    if conn_uri:
        new_conn = Connection(conn_id=conn_id, uri=conn_uri)
    else:
        new_conn = Connection(conn_id=conn_id,
                              conn_type=conn_type,
                              host=conn_host,
                              login=conn_login,
                              password=conn_password,
                              schema=conn_schema,
                              port=conn_port)
    if conn_extra is not None:
        new_conn.set_extra(conn_extra)

    session = settings.Session()
    session.add(new_conn)
    session.commit()
    return new_conn
    # msg = '\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
    # msg = msg.format(conn_id=new_conn.conn_id,
    #                  uri=conn_uri or
    #                      urlunparse((conn_type,
    #                                  '{login}:{password}@{host}:{port}'
    #                                  .format(login=conn_login or '',
    #                                          # TODO: should this return passwords?
    #                                          password=conn_password or '',
    #                                          host=conn_host or '',
    #                                          port=conn_port or ''),
    #                                  conn_schema or '', '', '', '')))
    #
    # return msg


def delete_connection(conn_id, delete_all=False):

    if conn_id is None:
        raise MissingArgument('\n\tTo delete a connection, you must provide a value for ' +
                              'the --conn_id flag.\n')

    session = settings.Session()
    to_delete = (session
                 .query(Connection)
                 .filter(Connection.conn_id == conn_id)).all()

    if len(to_delete) == 1:
        deleted_conn_id = to_delete[0].conn_id
        for conn in to_delete:
            session.delete(conn)
        session.commit()
        msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
        msg = msg.format(conn_id=deleted_conn_id)
        return msg
    elif len(to_delete) > 1:
        if delete_all:
            deleted_conn_id = to_delete[0].conn_id
            num_conns = len(to_delete)
            for conn in to_delete:
                session.delete(conn)
            session.commit()

            msg = '\n\tSuccessfully deleted {num_conns} connections with `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=deleted_conn_id, num_conns=num_conns)
            return msg
        else:
            msg = ('\n\tFound {num_conns} connection with ' +
                   '`conn_id`={conn_id}. Specify `delete_all=True` to remove all\n')
            msg = msg.format(conn_id=conn_id, num_conns=len(to_delete))
            return msg
    elif len(to_delete) == 0:
        msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
        msg = msg.format(conn_id=conn_id)
        return msg


def list_connections():
    session = settings.Session()
    conns = session.query(Connection.conn_id, Connection.conn_type,
                          Connection.host, Connection.port,
                          Connection.is_encrypted,
                          Connection.is_extra_encrypted,
                          Connection.extra).all()
    return conns

    # move this formating to the CLI!
    # conns = [map(reprlib.repr, conn) for conn in conns]
    # msg = tabulate(conns, ['Conn Id', 'Conn Type', 'Host', 'Port',
    #                        'Is Encrypted', 'Is Extra Encrypted', 'Extra'],
    #                tablefmt="fancy_grid")
    # if sys.version_info[0] < 3:
    #     msg = msg.encode('utf-8')
    # return msg


def update_connection(conn_id,
                      conn_uri=None,
                      conn_type=None,
                      conn_host=None,
                      conn_login=None,
                      conn_password=None,
                      conn_schema=None,
                      conn_port=None,
                      conn_extra=None):
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = list()
    invalid_args = list()
    if not conn_id:
        missing_args.append('conn_id')
    if conn_uri:
        if conn_type:
            invalid_args.append(conn_type)
        if conn_host:
            invalid_args.append(conn_host)
        if conn_login:
            invalid_args.append(conn_login)
        if conn_password:
            invalid_args.append(conn_password)
        if conn_schema:
            invalid_args.append(conn_schema)
        if conn_port:
            invalid_args.append(conn_port)
    elif not conn_type:
        missing_args.append('conn_uri or conn_type')

    if missing_args:
        msg = ('\n\tThe following args are required to update a connection:' +
               ' {missing!r}\n'.format(missing=missing_args))
        raise MissingArgument(msg)
    if invalid_args:
        msg = ('\n\tThe following args are not compatible with the ' +
               '--update flag and --conn_uri flag: {invalid!r}\n')
        msg = msg.format(invalid=invalid_args)
        raise MissingArgument(msg)

    # Update....
    session = settings.Session()
    try:
        to_update = (session
                     .query(Connection)
                     .filter(Connection.conn_id == conn_id)
                     .one())
    except exc.NoResultFound:
        msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
        msg = msg.format(conn_id=conn_id)
        raise ConnectionNotFound(msg)
    except exc.MultipleResultsFound:
        msg = ('\n\tUpdating multiple connections is not supported, Found multiple connections with ' +
               '`conn_id`={conn_id}\n')
        msg = msg.format(conn_id=conn_id)
        raise MultipleConnectionsFound(msg)
    else:

        # build a new connection to update from
        if conn_uri:
            temp_conn = Connection(conn_id='new_conn', uri=conn_uri)
        else:
            temp_conn = Connection(conn_id='new_conn',
                                   conn_type=conn_type,
                                   host=conn_host,
                                   login=conn_login,
                                   password=conn_password,
                                   schema=conn_schema,
                                   port=conn_port)
        if conn_extra is not None:
            temp_conn.set_extra(conn_extra)

        to_update.conn_type = temp_conn.conn_type or to_update.conn_type
        to_update.host = temp_conn.host or to_update.host
        to_update.login = temp_conn.login or to_update.login
        to_update.password = temp_conn.password or to_update.password
        to_update.schema = temp_conn.schema or to_update.schema
        to_update.port = temp_conn.port or to_update.port

        if temp_conn.extra is not None:
            to_update.set_extra(temp_conn.extra)
        session.commit()

        return to_update
        # msg = '\n\tSuccessfully updated `conn_id`={conn_id} : {uri}\n'
        # msg = msg.format(conn_id=to_update.conn_id,
        #                  uri=conn_uri or
        #                      urlunparse((conn_type,
        #                                  '{login}:{password}@{host}:{port}'
        #                                  .format(login=conn_login or '',
        #                                          password=conn_password or '',
        #                                          host=conn_host or '',
        #                                          port=conn_port or ''),
        #                                  conn_schema or '', '', '', '')))
        # return msg
