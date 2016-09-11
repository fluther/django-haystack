from datetime import timedelta
from optparse import make_option
import logging
import os
import multiprocessing
import time

from django import db
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import LabelCommand
from django.db import reset_queries
from django.utils.encoding import smart_str, force_unicode

from haystack import connections as haystack_connections
from haystack.query import SearchQuerySet

try:
    from django.utils.timezone import now
except ImportError:
    from datetime import datetime
    now = datetime.now


DEFAULT_BATCH_SIZE = None
DEFAULT_AGE = None
DEFAULT_MAX_RETRIES = 5
APP = 'app'
MODEL = 'model'

logger = multiprocessing.get_logger()
logger.name = __name__
logger.propagate = True

def worker(bits):
    # We need to reset the connections, otherwise the different processes
    # will try to share the connection, which causes things to blow up.
    from django.db import connections

    for alias, info in connections.databases.items():
        # We need to also tread lightly with SQLite, because blindly wiping
        # out connections (via ``... = {}``) destroys in-memory DBs.
        if not 'sqlite3' in info['ENGINE']:
            try:
                db.close_connection()
                if isinstance(connections._connections, dict):
                    del(connections._connections[alias])
                else:
                    delattr(connections._connections, alias)
            except KeyError:
                pass

    if bits[0] == 'do_update':
        func, model, start, end, total, using, start_date, end_date, verbosity, max_retries = bits
    elif bits[0] == 'do_remove':
        func, model, pks_seen, start, upper_bound, using, verbosity = bits
    else:
        return

    unified_index = haystack_connections[using].get_unified_index()
    index = unified_index.get_index(model)
    backend = haystack_connections[using].get_backend()


    if func == 'do_update':
        qs = index.build_queryset(start_date=start_date, end_date=end_date)
        do_update(backend, index, qs, start, end, total, verbosity=verbosity, max_retries=max_retries)
    elif bits[0] == 'do_remove':
        do_remove(backend, index, model, pks_seen, start, upper_bound, verbosity=verbosity)


def do_update(backend, index, qs, start, end, total, verbosity=1,
              max_retries=DEFAULT_MAX_RETRIES):
    # Get a clone of the QuerySet so that the cache doesn't bloat up
    # in memory. Useful when reindexing large amounts of data.
    small_cache_qs = qs.all()
    current_qs = small_cache_qs[start:end]

    is_parent_process = hasattr(os, 'getppid') and os.getpid() == os.getppid()
    if verbosity >= 2:
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            logger.info("  indexing %s - %d of %d." % (start + 1, end, total))
        else:
            logger.info("  indexing %s - %d of %d (by %s)." % (start + 1, end, total, os.getpid()))

    retries = 0
    while retries < max_retries:
        try:
            # FIXME: Get the right backend.
            backend.update(index, current_qs)
            if verbosity >= 2 and retries:
                logger.info('Completed indexing {} - {}, tried {}/{} times'.format(start + 1,
                                                                             end,
                                                                             retries + 1,
                                                                             max_retries))
            break
        except Exception as exc:
            # Catch all exceptions which do not normally trigger a system exit, excluding SystemExit and
            # KeyboardInterrupt. This avoids needing to import the backend-specific exception subclasses
            # from pysolr, elasticsearch, whoosh, requests, etc.
            retries += 1

            error_context = {'start': start + 1,
                             'end': end,
                             'retries': retries,
                             'max_retries': max_retries,
                             'pid': os.getpid(),
                             'exc': exc}

            error_msg = 'Failed indexing %(start)s - %(end)s (retry %(retries)s/%(max_retries)s): %(exc)s'
            if not is_parent_process:
                error_msg += ' (pid %(pid)s): %(exc)s'

            if retries >= max_retries:
                logger.error(error_msg, error_context, exc_info=True)
                raise
            elif verbosity >= 2:
                logger.warning(error_msg, error_context, exc_info=True)

            # If going to try again, sleep a bit before
            time.sleep(2 ** retries)


    # Clear out the DB connections queries because it bloats up RAM.
    reset_queries()


def do_remove(backend, index, model, pks_seen, start, upper_bound, verbosity=1):
    # Fetch a list of results.
    # Can't do pk range, because id's are strings (thanks comments
    # & UUIDs!).
    stuff_in_the_index = SearchQuerySet().models(model)[start:upper_bound]

    # Iterate over those results.
    for result in stuff_in_the_index:
        # Be careful not to hit the DB.
        if not smart_str(result.pk) in pks_seen:
            # The id is NOT in the small_cache_qs, issue a delete.
            if verbosity >= 2:
                logger.info("  removing %s." % result.pk)

            backend.remove(".".join([result.app_label, result.model_name, str(result.pk)]))


class Command(LabelCommand):
    help = "Freshens the index for the given app(s)."
    base_options = (
        make_option('-a', '--age', action='store', dest='age',
            default=DEFAULT_AGE, type='int',
            help='Number of hours back to consider objects new.'
        ),
        make_option('-s', '--start', action='store', dest='start_date',
            default=None, type='string',
            help='The start date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-e', '--end', action='store', dest='end_date',
            default=None, type='string',
            help='The end date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-b', '--batch-size', action='store', dest='batchsize',
            default=None, type='int',
            help='Number of items to index at once.'
        ),
        make_option('-r', '--remove', action='store_true', dest='remove',
            default=False, help='Remove objects from the index that are no longer present in the database.'
        ),
        make_option("-u", "--using", action="append", dest="using",
            default=[],
            help='Update only the named backend (can be used multiple times). '
                 'By default all backends will be updated.'
        ),
        make_option('-k', '--workers', action='store', dest='workers',
            default=0, type='int',
            help='Allows for the use multiple workers to parallelize indexing. Requires multiprocessing.'
        ),
        make_option('-t', '--max-retries', action='store', dest='max_retries',
                    default=DEFAULT_MAX_RETRIES, type='int',
                    help='Maximum number of attempts to write to the backend when an error occurs.'),
    )
    option_list = LabelCommand.option_list + base_options

    def handle(self, *items, **options):
        self.verbosity = int(options.get('verbosity', 1))
        self.batchsize = options.get('batchsize', DEFAULT_BATCH_SIZE)
        self.start_date = None
        self.end_date = None
        self.remove = options.get('remove', False)
        self.workers = int(options.get('workers', 0))
        self.max_retries = options.get('max_retries', DEFAULT_MAX_RETRIES)

        self.backends = options.get('using')
        if not self.backends:
            self.backends = haystack_connections.connections_info.keys()

        age = options.get('age', DEFAULT_AGE)
        start_date = options.get('start_date')
        end_date = options.get('end_date')

        if age is not None:
            self.start_date = now() - timedelta(hours=int(age))

        if start_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.start_date = dateutil_parse(start_date)
            except ValueError:
                pass

        if end_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.end_date = dateutil_parse(end_date)
            except ValueError:
                pass

        if not items:
            from django.db.models import get_app
            # Do all, in an INSTALLED_APPS sorted order.
            items = []

            for app in settings.INSTALLED_APPS:
                try:
                    app_label = app.split('.')[-1]
                    loaded_app = get_app(app_label)
                    items.append(app_label)
                except:
                    # No models, no problem.
                    pass

        return super(Command, self).handle(*items, **options)

    def is_app_or_model(self, label):
        label_bits = label.split('.')

        if len(label_bits) == 1:
            return APP
        elif len(label_bits) == 2:
            return MODEL
        else:
            raise ImproperlyConfigured("'%s' isn't recognized as an app (<app_label>) or model (<app_label>.<model_name>)." % label)

    def get_models(self, label):
        from django.db.models import get_app, get_models, get_model
        app_or_model = self.is_app_or_model(label)

        if app_or_model == APP:
            app_mod = get_app(label)
            return get_models(app_mod)
        else:
            app_label, model_name = label.split('.')
            return [get_model(app_label, model_name)]

    def handle_label(self, label, **options):
        for using in self.backends:
            try:
                self.update_backend(label, using)
            except:
                logging.exception("Error updating %s using %s ", label, using)
                raise

    def update_backend(self, label, using):
        from haystack.exceptions import NotHandled

        backend = haystack_connections[using].get_backend()
        unified_index = haystack_connections[using].get_unified_index()

        for model in self.get_models(label):
            try:
                index = unified_index.get_index(model)
            except NotHandled:
                if self.verbosity >= 2:
                    logger.debug("Skipping '%s' - no index." % model)
                continue

            if self.workers > 0:
                # workers resetting connections leads to references to models / connections getting
                # stale and having their connection disconnected from under them. Resetting before
                # the loop continues and it accesses the ORM makes it better.
                db.close_connection()

            qs = index.build_queryset(using=using, start_date=self.start_date,
                                      end_date=self.end_date)

            total = qs.count()

            if self.verbosity >= 1:
                logger.info(u"Indexing %d %s" % (total, force_unicode(model._meta.verbose_name_plural)))

            pks_seen = set([smart_str(pk) for pk in qs.values_list('pk', flat=True)])
            batch_size = self.batchsize or backend.batch_size

            if self.workers > 0:
                ghetto_queue = []

            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)

		if self.workers == 0:
                    do_update(backend, index, qs, start, end, total, verbosity=self.verbosity, max_retries=self.max_retries)
                else:
                    ghetto_queue.append(('do_update', model, start, end, total, using, self.start_date, self.end_date,
                                         self.verbosity, self.max_retries))


            if self.workers > 0:
                pool = multiprocessing.Pool(self.workers)
                pool.map(worker, ghetto_queue)
                pool.terminate()

            if self.remove:
                if self.start_date or self.end_date or total <= 0:
                    # They're using a reduced set, which may not incorporate
                    # all pks. Rebuild the list with everything.
                    qs = index.index_queryset().values_list('pk', flat=True)
                    pks_seen = set([smart_str(pk) for pk in qs])
                    total = len(pks_seen)

                if self.workers > 0:
                    ghetto_queue = []

                for start in range(0, total, batch_size):
                    upper_bound = start + batch_size

                    if self.workers == 0:
                        do_remove(backend, index, model, pks_seen, start, upper_bound)
                    else:
                        ghetto_queue.append(('do_remove', model, pks_seen, start, upper_bound, using, self.verbosity))

                if self.workers > 0:
                    pool = multiprocessing.Pool(self.workers)
                    pool.map(worker, ghetto_queue)
                    pool.terminate()
