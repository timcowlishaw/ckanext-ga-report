import logging
import datetime
import os

from pylons import config

from ckan.lib.cli import CkanCommand
# No other CKAN imports allowed until _load_config is run,
# or logging is disabled


class InitDB(CkanCommand):
    """Initialise the extension's database tables
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        self._load_config()

        import ckan.model as model
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)
        log = logging.getLogger('ckanext.ga_report')

        import ga_model
        ga_model.init_tables()
        log.info("DB tables are setup")


class GetAuthToken(CkanCommand):
    """ Get's the Google auth token

    Usage: paster getauthtoken <credentials_file>

    Where <credentials_file> is the file name containing the details
    for the service (obtained from https://code.google.com/apis/console).
    By default this is set to credentials.json
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 1

    def command(self):
        """
        In this case we don't want a valid service, but rather just to
        force the user through the auth flow. We allow this to complete to
        act as a form of verification instead of just getting the token and
        assuming it is correct.
        """
        from ga_auth import init_service
        init_service('token.dat',
                      self.args[0] if self.args
                                   else 'credentials.json')

class FixTimePeriods(CkanCommand):
    """
    Fixes the 'All' records for GA_Urls

    It is possible that older urls that haven't recently been visited
    do not have All records.  This command will traverse through those
    records and generate valid All records for them.
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def __init__(self, name):
        super(FixTimePeriods, self).__init__(name)

    def command(self):
        import ckan.model as model
        from ga_model import post_update_url_stats
        self._load_config()
        model.Session.remove()
        model.Session.configure(bind=model.meta.engine)

        log = logging.getLogger('ckanext.ga_report')

        log.info("Updating 'All' records for old URLs")
        post_update_url_stats(print_progress=True)
        log.info("Processing complete")



class LoadAnalytics(CkanCommand):
    """Get data from Google Analytics API and save it
    in the ga_model

    Usage: paster loadanalytics <time-period>

    Where <time-period> is:
        all         - data for all time
        latest      - (default) just the 'latest' data
        YYYY-MM     - just data for the specific month
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def __init__(self, name):
        super(LoadAnalytics, self).__init__(name)
        self.parser.add_option('-d', '--delete-first',
                               action='store_true',
                               default=False,
                               dest='delete_first',
                               help='Delete data for the period first')
        self.parser.add_option('-s', '--skip_url_stats',
                               action='store_true',
                               default=False,
                               dest='skip_url_stats',
                               help='Skip the download of URL data - just do site-wide stats')
        self.token = ""

    def command(self):
        self._load_config()

        from download_analytics import DownloadAnalytics
        from ga_auth import (init_service, get_profile_id)

        ga_token_filepath = os.path.expanduser(config.get('googleanalytics.token.filepath', ''))
        if not ga_token_filepath:
            print 'ERROR: In the CKAN config you need to specify the filepath of the ' \
                  'Google Analytics token file under key: googleanalytics.token.filepath'
            return

        try:
            self.token, svc = init_service(ga_token_filepath, None)
        except TypeError:
            print ('Have you correctly run the getauthtoken task and '
                   'specified the correct token file in the CKAN config under '
                   '"googleanalytics.token.filepath"?')
            return

        downloader = DownloadAnalytics(svc, self.token, profile_id=get_profile_id(svc),
                                       delete_first=self.options.delete_first,
                                       skip_url_stats=self.options.skip_url_stats,
                                       print_progress=True)

        time_period = self.args[0] if self.args else 'latest'
        if time_period == 'all':
            downloader.all_()
        elif time_period == 'latest':
            downloader.latest()
        else:
            # The month to use
            for_date = datetime.datetime.strptime(time_period, '%Y-%m')
            downloader.specific_month(for_date)

class GenerateDatasetsCsv(CkanCommand):
    """Generate the per-dataset statistics CSV 

    Usage: paster generatedatasetscsv <output-path>
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 1 

    def command(self):
        self._load_config()
        import csv

        with open(self.args[0], "wb") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["Dataset Title", "Dataset Name", "Views", "Visits", "Resource downloads", "Period Name"])
            packages = self._get_packages()
            for package,view,visit,downloads in packages:
                writer.writerow([package.title.encode('utf-8'),
                                package.name.encode('utf-8'),
                                view,
                                visit,
                                downloads,
                                'All'])
    def _get_packages(self):
        import ckan.model as model
        from ga_model import GA_Url, GA_Stat
        packages = []
        entries = model.Session.query(GA_Url, model.Package).filter(model.Package.name ==GA_Url.package_id).filter(GA_Url.url.like('/dataset/%')).filter(GA_Url.period_name=='All').order_by('ga_url.pageviews::int desc').all()
        for entry,package in entries:
            if package:
                dls = model.Session.query(GA_Stat).filter(GA_Stat.stat_name=='Downloads').filter(GA_Stat.key==package.name)
                downloads = 0
                for x in dls:
                    downloads += int(x.value)
                packages.append((package, entry.pageviews, entry.visits, downloads))
            else:
                log.warning('Could not find package associated package')

        return packages


class GenerateReferrersCsv(CkanCommand):
    """Generate the top referred datasets statistics CSV
    Usage: paster generatereferrerscsv <output-path>
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 1 
    n_top_ranked = 5
    
    def command(self):
        self._load_config()
        import csv

        with open(self.args[0], "wb") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["Dataset Title", "Dataset Name", "Referred Visits", "Period Name"])
            data = self._get_data()
            for (title, name, shares, period) in data:
                writer.writerow([
                    title.encode("utf-8"),
                    name.encode("utf-8"),
                    shares,
                    period])

    def _get_data(self):
        return self._get_data_for_periods() + self._get_top_datasets(self.n_top_ranked);

    def _get_data_for_periods(self):
        return [dataset for period in self._get_periods()
                 for dataset in self._get_top_datasets(self.n_top_ranked, period)]

    def _get_top_datasets(self, n, period="All"):
        import ckan.model as model
        from ga_model import GA_ReferralStat, GA_Url
        from sqlalchemy import func
        from sqlalchemy.sql.expression import literal_column
        query = (model.Session.query(
            func.min(model.Package.title),
            func.min(model.Package.name),
            func.sum(GA_ReferralStat.count),
            literal_column("'" + period + "'").label("period"))
            .join(GA_Url, model.Package.name == GA_Url.package_id)
            .join(GA_ReferralStat, GA_Url.url == GA_ReferralStat.url)
            .filter(GA_Url.url.like("/dataset/%"))
            .group_by(GA_Url.url)
            .order_by(func.sum(GA_ReferralStat.count).desc()))
        if period != "All":
            query = query.filter(GA_ReferralStat.period_name==period)
        return query.limit(n).all()
    
    def _get_periods(self):
        import ckan.model as model
        from ga_model import GA_ReferralStat
        return [period for (period,) in model.Session.query(GA_ReferralStat.period_name).order_by(GA_ReferralStat.period_name).distinct().all()]

