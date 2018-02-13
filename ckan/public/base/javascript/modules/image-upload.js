 /* Image Upload
 *
 */
this.ckan.module('image-upload', function($) {
  return {
    /* options object can be extended using data-module-* attributes */
    options: {
      is_url: false,
      is_upload: false,
      field_upload: 'image_upload',
      field_url: 'image_url',
      field_clear: 'clear_upload',
      field_name: 'name',
      field_query: 'query',
      upload_label: ''
    },

    /* Should be changed to true if user modifies resource's name
     *
     * @type {Boolean}
     */
    _nameIsDirty: false,

    /* Initialises the module setting up elements and event listeners.
     *
     * Returns nothing.
     */
    initialize: function () {
      $.proxyAll(this, /_on/);
      var options = this.options;

      // firstly setup the fields
      var field_upload = 'input[name="' + options.field_upload + '"]';
      var field_url = 'input[name="' + options.field_url + '"]';
      var field_clear = 'input[name="' + options.field_clear + '"]';
      var field_name = 'input[name="' + options.field_name + '"]';
      var field_query = 'textarea[name="' + options.field_query + '"]';

      this.input = $(field_upload, this.el);
      this.field_url = $(field_url, this.el).parents('.control-group');
      this.field_image = this.input.parents('.control-group');
      this.field_url_input = $('input', this.field_url);
      this.field_name = this.el.parents('form').find(field_name);
      // this is the location for the upload/link data/image label
      this.label_location = $('label[for="field-image-url"]');
      // determines if the resource is a data resource
      this.is_data_resource = (this.options.field_url === 'url') && (this.options.field_upload === 'upload');
      this.field_query = $(field_query).parents('.control-group');
      this.field_query_input = $(field_query)[0];
      this.field_format = $('input[name="format"]').parents('.control-group');

      // Is there a clear checkbox on the form already?
      var checkbox = $(field_clear, this.el);
      if (checkbox.length > 0) {
        checkbox.parents('.control-group').remove();
      }

      // Adds the hidden clear input to the form
      this.field_clear = $('<input type="hidden" name="' + options.field_clear +'">')
        .appendTo(this.el);

      // Button to use a query for this resource
      var removeText = this._('Remove');
      if (this.field_query_input) {
        this.button_query = $('<a href="javascript:;" class="btn">' +
                           '<i class="fa fa-filter"></i>' +
                           this._('Query') + '</a>')
          .prop('title', this._('Query data from other resources'))
          .on('click', this._onQuery)
          .insertAfter(this.input);
        $('<a href="javascript:;" class="btn btn-danger btn-remove-url" style="top:-1.7em">'
          + removeText + '</a>')
          .prop('title', removeText)
          .on('click', this._onRemove)
          .insertBefore(this.field_query_input);
        $(this.field_query)
          .on('change', this._onQueryChange)
      }

      // Button to set the field to be a URL
      this.button_url = $('<a href="javascript:;" class="btn">' +
                          '<i class="fa fa-globe"></i>' +
                          this._('Link') + '</a>')
        .prop('title', this._('Link to a URL on the internet (you can also link to an API)'))
        .on('click', this._onFromWeb)
        .insertAfter(this.input);

      // Button to attach local file to the form
      this.button_upload = $('<a href="javascript:;" class="btn">' +
                             '<i class="fa fa-cloud-upload"></i>' +
                             this._('Upload') + '</a>')
        .insertAfter(this.input);

      // Button for resetting the form when there is a URL set
      $('<a href="javascript:;" class="btn btn-danger btn-remove-url">'
        + removeText + '</a>')
        .prop('title', removeText)
        .on('click', this._onRemove)
        .insertBefore(this.field_url_input);

      // Update the main label (this is displayed when no data/image has been uploaded/linked)
      $('label[for="field-image-upload"]').text(options.upload_label || this._('Image'));

      // Setup the file input
      this.input
        .on('mouseover', this._onInputMouseOver)
        .on('mouseout', this._onInputMouseOut)
        .on('change', this._onInputChange)
        .prop('title', this._('Upload a file on your computer'))
        .css('width', this.button_upload.outerWidth());

      // Fields storage. Used in this.changeState
      this.fields = $('<i />')
        .add(this.button_upload)
        .add(this.button_url)
        .add(this.input)
        .add(this.field_url)
        .add(this.field_image)
        .add(this.field_query);

      // Disables autoName if user modifies name field
      this.field_name
        .on('change', this._onModifyName);
      // Disables autoName if resource name already has value,
      // i.e. we on edit page
      if (this.field_name.val()){
        this._nameIsDirty = true;
      }

      if (this.field_query_input && this.field_query_input.value) {
        this._showOnlyQuery();
      } else if (options.is_url) {
        this._showOnlyFieldUrl();

        this._updateUrlLabel(this._('URL'));
      } else if (options.is_upload) {
        this._showOnlyFieldUrl();

        this.field_url_input.prop('readonly', true);
        // If the data is an uploaded file, the filename will display rather than whole url of the site
        var filename = this._fileNameFromUpload(this.field_url_input.val());
        this.field_url_input.val(filename);

        this._updateUrlLabel(this._('File'));
      } else {
        this._showOnlyButtons();
      }
    },

    /* Quick way of getting just the filename from the uri of the resource data
     *
     * url - The url of the uploaded data file
     *
     * Returns String.
     */
    _fileNameFromUpload: function(url) {
      // remove fragment (#)
      url = url.substring(0, (url.indexOf("#") === -1) ? url.length : url.indexOf("#"));
      // remove query string
      url = url.substring(0, (url.indexOf("?") === -1) ? url.length : url.indexOf("?"));
      // extract the filename
      url = url.substring(url.lastIndexOf("/") + 1, url.length);

      return url; // filename
    },

    /* Update the `this.label_location` text
     *
     * If the upload/link is for a data resource, rather than an image,
     * the text for label[for="field-image-url"] will be updated.
     *
     * label_text - The text for the label of an uploaded/linked resource
     *
     * Returns nothing.
     */
    _updateUrlLabel: function(label_text) {
      if (! this.is_data_resource) {
        return;
      }

      this.label_location.text(label_text);
    },

    /* Event listener for when someone sets the field to URL mode
     *
     * Returns nothing.
     */
    _onFromWeb: function() {
      this._showOnlyFieldUrl();

      this.field_url_input.focus()
        .on('blur', this._onFromWebBlur);

      if (this.options.is_upload) {
        this.field_clear.val('true');
      }

      this._updateUrlLabel(this._('URL'));
    },

    _onQuery: function() {
      this._showOnlyQuery();
      this.field_query_input.focus();
    },

    /* Event listener for resetting the field back to the blank state
     *
     * Returns nothing.
     */
    _onRemove: function() {
      this._showOnlyButtons();

      this.field_url_input.val('');
      this.field_url_input.prop('readonly', false);

      this.field_clear.val('true');

      if (this.field_query_input) {
        this.field_query_input.value = '';
      }
    },

    /* Event listener for when someone chooses a file to upload
     *
     * Returns nothing.
     */
    _onInputChange: function() {
      var file_name = this.input.val().split(/^C:\\fakepath\\/).pop();
      this.field_url_input.val(file_name);
      this.field_url_input.prop('readonly', true);

      this.field_clear.val('');

      this._showOnlyFieldUrl();

      this._autoName(file_name);

      this._updateUrlLabel(this._('File'));
    },

    /* Event listener for when someone starts to enter a query
     *
     * Returns nothing.
     */
    _onQueryChange: function() {
      this._autoName('Query');
    },


    /* Show only the buttons, hiding all others
     *
     * Returns nothing.
     */
    _showOnlyButtons: function() {
      this.fields.hide();
      this.button_upload
        .add(this.field_image)
        .add(this.button_url)
        .add(this.input)
        .show();
      this.field_format.show();
    },

    /* Show only the URL field, hiding all others
     *
     * Returns nothing.
     */
    _showOnlyFieldUrl: function() {
      this.fields.hide();
      this.field_url.show();
      this.field_format.show();
    },

    /* Show only the Query field, hiding all others
     *
     * Returns nothing.
     */
    _showOnlyQuery: function() {
      this.fields.hide();
      this.field_query.show();
      this.field_format.hide();
    },
    /* Event listener for when a user mouseovers the hidden file input
     *
     * Returns nothing.
     */
    _onInputMouseOver: function() {
      this.button_upload.addClass('hover');
    },

    /* Event listener for when a user mouseouts the hidden file input
     *
     * Returns nothing.
     */
    _onInputMouseOut: function() {
      this.button_upload.removeClass('hover');
    },

    /* Event listener for changes in resource's name by direct input from user
     *
     * Returns nothing
     */
    _onModifyName: function() {
      this._nameIsDirty = true;
    },

    /* Event listener for when someone loses focus of URL field
     *
     * Returns nothing
     */
    _onFromWebBlur: function() {
      var url = this.field_url_input.val().match(/([^\/]+)\/?$/)
      if (url) {
        this._autoName(url.pop());
      }
    },

    /* Automatically add file name into field Name
     *
     * Select by attribute [name] to be on the safe side and allow to change field id
     * Returns nothing
     */
     _autoName: function(name) {
        if (!this._nameIsDirty){
          this.field_name.val(name);
        }
     }
  };
});
