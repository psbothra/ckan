# encoding: utf-8

import unicodecsv
import ckan.lib.helpers as h
from ckan.plugins.toolkit import (
    Invalid,
    ObjectNotFound,
    NotAuthorized,
    get_action,
    get_validator,
    _,
    request,
    response,
    BaseController,
    abort,
    render,
    c,
)
from ckanext.datastore.writer import (
    csv_writer,
    tsv_writer,
    json_writer,
    xml_writer,
)
from ckan.logic import (
    tuplize_dict,
    parse_params,
)
import ckan.lib.navl.dictization_functions as dict_fns
from itertools import izip_longest

int_validator = get_validator('int_validator')
boolean_validator = get_validator('boolean_validator')

DUMP_FORMATS = 'csv', 'tsv', 'json', 'xml'
PAGINATE_BY = 32000


class DatastoreController(BaseController):
    def dump(self, resource_id):
        try:
            offset = int_validator(request.GET.get('offset', 0), {})
        except Invalid as e:
            abort(400, u'offset: ' + e.error)
        try:
            limit = int_validator(request.GET.get('limit'), {})
        except Invalid as e:
            abort(400, u'limit: ' + e.error)
        bom = boolean_validator(request.GET.get('bom'), {})
        fmt = request.GET.get('format', 'csv')

        def start_writer(fields):
            if fmt == 'csv':
                return csv_writer(response, fields, resource_id, bom)
            if fmt == 'tsv':
                return tsv_writer(response, fields, resource_id, bom)
            if fmt == 'json':
                return json_writer(response, fields, resource_id, bom)
            if fmt == 'xml':
                return xml_writer(response, fields, resource_id, bom)
            abort(400, _(
                u'format: must be one of %s') % u', '.join(DUMP_FORMATS))

        def result_page(offset, limit):
            try:
                return get_action('datastore_search')(None, {
                    'resource_id': resource_id,
                    'limit':
                        PAGINATE_BY if limit is None
                        else min(PAGINATE_BY, limit),
                    'offset': offset,
                    })
            except ObjectNotFound:
                abort(404, _('DataStore resource not found'))

        result = result_page(offset, limit)
        columns = [x['id'] for x in result['fields']]

        with start_writer(result['fields']) as wr:
            while True:
                if limit is not None and limit <= 0:
                    break

                for record in result['records']:
                    wr.writerow([record[column] for column in columns])

                if len(result['records']) < PAGINATE_BY:
                    break
                offset += PAGINATE_BY
                if limit is not None:
                    limit -= PAGINATE_BY

                result = result_page(offset, limit)

    def dictionary(self, id, resource_id):
        u'''data dictionary view: show/edit field labels and descriptions'''

        try:
            # resource_edit_base template uses these
            c.pkg_dict = get_action('package_show')(
                None, {'id': id})
            c.resource = get_action('resource_show')(
                None, {'id': resource_id})
            rec = get_action('datastore_search')(None, {
                'resource_id': resource_id,
                'limit': 0})
        except (ObjectNotFound, NotAuthorized):
            abort(404, _('Resource not found'))

        fields = [f for f in rec['fields'] if not f['id'].startswith('_')]

        if request.method == 'POST':
            data = dict_fns.unflatten(tuplize_dict(parse_params(
                request.params)))
            info = data.get(u'info')
            if not isinstance(info, list):
                info = []
            info = info[:len(fields)]

            get_action('datastore_create')(None, {
                'resource_id': resource_id,
                'force': True,
                'fields': [{
                    'id': f['id'],
                    'type': f['type'],
                    'info': fi if isinstance(fi, dict) else {}
                    } for f, fi in izip_longest(fields, info)]})

            h.flash_success(_('Data Dictionary saved. Any type overrides will '
                              'take effect when the resource is next uploaded '
                              'to DataStore'))

            h.redirect_to(
                controller='ckanext.datastore.controller:DatastoreController',
                action='dictionary',
                id=id,
                resource_id=resource_id)

        return render(
            'datastore/dictionary.html',
            extra_vars={'fields': fields})

    def dictionary_download(self, resource_id):
        try:
            resource_datastore = get_action('datastore_search')(None, {
                'resource_id': resource_id,
                'limit': 0})
        except (ObjectNotFound, NotAuthorized):
            abort(404, _('Resource not found'))

        fields = [f for f in resource_datastore['fields'] if not f['id'].startswith('_')]
        header = ['column','type','label','description']

        if hasattr(response, u'headers'):
            response.headers['Content-Type'] = b'text/csv; charset=utf-8'
            response.headers['Content-disposition'] = (
                b'attachment; filename="{name}.csv"'.format(name=resource_id))

        wr = unicodecsv.writer(response, encoding=u'utf-8')
        wr.writerow(col for col in header)
        for field in fields:
            field_info = field.get('info',{})
            row = [field['id'], field['type'], field_info.get('label',''), field_info.get('notes','')]
            wr.writerow(item for item in row)
