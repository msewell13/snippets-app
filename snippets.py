import logging
import argparse
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename='snippets.log', level=logging.DEBUG)

logging.debug('Connecting to PostgreSQL')
connection = psycopg2.connect(database='snippets')
logging.debug('Database connection established')


def put(name, snippet):
    '''Store a snippet with the associated name.
    Returns the name and the snippet.'''
    logging.info('Storing snippet {!r}: {!r}'.format(name, snippet))
    
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute('insert into snippets values (%s, %s)', (name, snippet))
    except psycopg2.IntegrityError as e:
        with connection, connection.cursor() as cursor:
            cursor.execute('update snippets set message=%s where keyword=%s', (snippet, name))
    logging.debug('Snippet stored successfully')
    return name, snippet
    
    
def get(name):
    '''Retrieve the snippet with the given name.
    If there is no such snippet, return '404: Snippet Not Found'.
    Returns the snippet'''
    logging.info('Retrieved snippet {!r}:'.format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    if not row:
        '''No snippet was found with that name'''
        return '404: Snippet Not Found'
    return row[1]
    
    
def main():
    '''Main function'''
    logging.info('Constructing parser')
    parser = argparse.ArgumentParser(description='Store and receive snippets of text')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Subparcer for the put command
    logging.info('Constructing put subparcer')
    put_parser = subparsers.add_parser('put', help='Store a snippet')
    put_parser.add_argument('name', help='Name of the snippet')
    put_parser.add_argument('snippet', help='Snippet text')
    
    # Subparcer for the get command
    logging.info('Constructing get subparcer')
    get_parser = subparsers.add_parser('get', help='get a snippet')
    get_parser.add_argument('name', help='Name of the snippet')
    
    arguments = parser.parse_args()
    #Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop('command')
    
    if command == 'put':
        name, snippet = put(**arguments)
        print('Stored {!r} as {!r}'.format(snippet, name))
    elif command == 'get':
        snippet = get(**arguments)
        print('Retrieved snippet: {!r}'.format(snippet))
    
    
    
if __name__ == '__main__':
    main()