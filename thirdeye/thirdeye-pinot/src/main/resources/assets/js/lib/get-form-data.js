function getDataSetList(){

    var url = "/dashboard/data/datasets";
    getData(url).done( function(data){

        var errorMessage = $("#" + hash.view + "-time-input-form-error p");
        var errorAlert = $("#" + hash.view + "-time-input-form-error");

        if (!data) {
            errorMessage.html("No dataset list arrived from the server. Error: data = " + data);
            errorAlert.fadeIn(100);
            errorAlert.attr("data-error-source", "datasetlist");
            return
        } else {
            $("#"+ hash.view +"-time-input-form-error[data-error-source= 'datasetlist']").hide()
        }

        /* Handelbars template for datasets dropdown */
        var result_datasets_template = HandleBarsTemplates.template_datasets(data);
        $(".landing-dataset").each(function(){ $(this).html(result_datasets_template)});

        $(".selected-dataset").text("Select dataset");


        if (hash.hasOwnProperty('dataset')) {
            //Populate the selected item on the form element
            $(".selected-dataset").text(hash.dataset);
            $(".selected-dataset").attr("value", hash.dataset);

            //Trigger AJAX calls
            //get the latest available data timestamp of a dataset
            getAllFormData()
        }

    });
};

function getAllFormData(){

    //Todo: remove these 2 global variables and work with $.when and deferreds
    window.responseDataPopulated = 0;
    window.numFormComponents = 4;
    getDatasetConfig();
    getDashboardList();
    getMetricList();
    getDimensionNFilterList();
}

function getDashboardList(){

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/dashboards?dataset=" + hash.dataset;

    getData(url).done( function(data){

        /* Create dashboard dropdown */
        var dashboardListHtml = "";
        for(var i= 0, len = data.length; i<len; i++){
            dashboardListHtml += "<li class='dashboard-option' rel='dashboard' value='"+ data[i] +"'><a href='#'>"+ data[i] +"</a></li>";
        }
        $("#dashboard-list").html(dashboardListHtml);
        $("#selected-dashboard").text("Select dashboard");

        window.responseDataPopulated++
        formComponentPopulated()
    });
};

function getMetricList() {

    //Create metric dropdown
    var url = "/dashboard/data/metrics?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        var errorMessage = $("#" + hash.view + "-time-input-form-error p");
        var errorAlert = $("#" + hash.view + "-time-input-form-error");
        if (!data) {
            errorMessage.html("No metrics available in the server. Error: data = " + data);
            errorAlert.fadeIn(100);
            return
        } else {
            errorAlert.hide()
        }


        /* Create metrics dropdown */
        var metricListHtml = "";
        for (var i = 0, len = data.length; i < len; i++) {
            metricListHtml += "<li class='metric-option' rel='metrics' value='" + data[i] + "'><a href='#' class='uk-dropdown-close'>" + data[i] + "</a></li>";
        }
        $(".metric-list").html(metricListHtml);


        window.responseDataPopulated++
        formComponentPopulated()
    });
}


function getDimensionNFilterList() {

    //Create dimension dropdown and filters
    var url = "/dashboard/data/filters?dataset=" + hash.dataset;
    getData(url).done(function (data) {


        var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
        var errorAlert = $("#"+ hash.view +"-time-input-form-error");
        if(!data){
            errorMessage.html("No dimension or dimension values available. Error: dimension data = " + data);
            errorAlert.fadeIn(100);
            return
        }else{
            errorAlert.hide()
        }


        /* Create dimensions and filter dimensions dropdown */
        var dimensionListHtml = "";
        var filterDimensionListHtml = "";

        //Global - public
        datasetDimensions = []

        for (var k in  data) {
            dimensionListHtml += "<li class='dimension-option' rel='dimensions' value='" + k + "'><a href='#' class='uk-dropdown-close'>" + k + "</a></li>";
            filterDimensionListHtml += "<li class='filter-dimension-option' value='" + k + "'><a href='#' class='radio-options'>" + k + "</a></li>";
            datasetDimensions.push(k)
        }

        $(".dimension-list").html(dimensionListHtml);

        //append filter dimension list
        $(".filter-dimension-list").html(filterDimensionListHtml);

        /* Handelbars template for dimensionvalues in filter dropdown */
        var result_filter_dimension_value_template = HandleBarsTemplates.template_filter_dimension_value(data)
        $(".dimension-filter").each(function(){
            $(this).after(result_filter_dimension_value_template)
        });


        $(".filter-dimension-option:first-of-type").each(function(){
            $(this).click();
            $(".radio-options",this).click();
        });


        window.responseDataPopulated++
        formComponentPopulated()
    });
};


function getDatasetConfig() {

    window.datasetConfig = {}

    //Till the endpoint is ready no ajax call is triggered and works with  hardcoded data in local data variable
    var url = "/dashboard/data/info?dataset=" + hash.dataset;

    getData(url).done(function (data) {

        var errorMessage = $("#"+ hash.view +"-time-input-form-error p");
        var errorAlert = $("#"+ hash.view +"-time-input-form-error");
        if(!data){
            errorMessage.html("No dataset info available. Error: data/info?dataset data = " + data);
            errorAlert.attr("data-error-source", "datasetinfo");
            errorAlert.fadeIn(100);
            return
        }else{
            $("#"+ hash.view +"-time-input-form-error[data-error-source= 'datasetinfo']").hide()
        }

        /** MIN MAX DATE TIME **/

        //global
        window.datasetConfig.maxMillis = parseInt(data["maxTime"]);
        var maxMillis = window.datasetConfig.maxMillis

        var currentStartDateTime = moment(maxMillis).add(-1, 'days');
        var currentStartDateString = currentStartDateTime.format("YYYY-MM-DD");
        var currentStartTimeString = currentStartDateTime.format("HH" + ":00");

        //Max date time
        var currentEndDateTime = moment(maxMillis);
        var currentEndDateString = currentEndDateTime.format("YYYY-MM-DD");
        var currentEndTimeString = currentEndDateTime.format("HH:00");

        //Populate WoW date
        var baselineStartDateTime = currentStartDateTime.add(-7,'days');
        var baselineStartDateString = baselineStartDateTime.format("YYYY-MM-DD");
        var baselineStartTimeString = currentStartTimeString;

        //Populate WoW time
        var baselineEndDateTime = currentEndDateTime.add(-7, 'days');
        var baselineEndDateString = baselineEndDateTime.format("YYYY-MM-DD");
        var baselineEndTimeString = currentEndTimeString;

        $(".current-start-date").text(currentStartDateString);
        $(".current-end-date").text(currentEndDateString);

        $(".current-start-time").text(currentStartTimeString);
        $(".current-end-time").text(currentEndTimeString);

        $(".baseline-start-date").text(baselineStartDateString);
        $(".baseline-end-date").text(baselineEndDateString);

        $(".baseline-start-time").text(baselineStartTimeString);
        $(".baseline-end-time").text(baselineEndTimeString);

        $(".current-start-date-input").val(currentStartDateString);
        $(".current-end-date-input").val(currentEndDateString);

        $(".current-start-time-input").val(currentStartTimeString);
        $(".current-end-time-input").val(currentEndTimeString);

        $(".baseline-start-date-input").val(baselineStartDateString);
        $(".baseline-end-date-input").val(baselineEndDateString);

        $(".baseline-start-time-input").val(baselineStartTimeString);
        $(".baseline-end-time-input").val(baselineEndTimeString);

        //Set the max date on the datepicker dropdowns
        var maxDate = moment(maxMillis).format("YYYY-MM-DD");
        UIkit.datepicker(UIkit.$('.current-start-date-input'), { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.current-end-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.baseline-start-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });
        UIkit.datepicker(UIkit.$('.baseline-end-date-input'),  { maxDate: maxDate, format:'YYYY-MM-DD' });

        //Add max and min time as a label time selection dropdown var minMillis = data["minTime"];
        var maxDateTime = maxMillis ? moment(maxMillis).format("YYYY-MM-DD h a") : "n.a.";
        $(".max-time").text(maxDateTime);

        //todo add min time to info endpoint
        var minMillis = parseInt(data["minTime"]);
        var minDateTime = minMillis ? moment(minMillis).format("YYYY-MM-DD h a") : "n.a.";
        $(".min-time").text(minDateTime);


        //if no data available for today 12am hide today and yesterday option from time selection
        //if( moment(parseInt(maxMillis)) < moment(parseInt(Date.now())) ){
        var dateToday = moment(Date.now()).format("YYYY-MM-DD")

        if(  moment(dateToday).isAfter( moment(maxDate) )  ){

            $(".current-date-range-option[value='today']").addClass("uk-hidden")
            $(".current-date-range-option[value='yesterday']").addClass("uk-hidden");

        }


        /**CONFIG: DATA GRANULARITY **/
        if(data["dataGranularity"]){


            window.datasetConfig.dataGranularity = data["dataGranularity"];
            var dataGranularity = window.datasetConfig.dataGranularity;

            //Todo: you may remove the following if else if the set of values are known
            if(dataGranularity.toLowerCase().indexOf("minutes") > -1){
                dataGranularity = "MINUTES";
            }else if(dataGranularity.toLowerCase().indexOf("hours") > -1){
                dataGranularity = "HOURS";
            }else if(dataGranularity.toLowerCase().indexOf("days") > -1){
                dataGranularity = "DAYS";
            }

            switch(dataGranularity){

                case "MINUTES":
                    $(".baseline-aggregate[unit='10_MINUTES']").removeClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").addClass("vertical");

                break;
                case "HOURS":
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").removeClass("vertical");

                break;
                case "DAYS":
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").addClass("uk-hidden");
                    //Select DAYS as default granularity
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-active");
                    $(".baseline-aggregate[unit='DAYS']").addClass("uk-active");
                    $(".granularity-btn-group").removeClass("vertical");

                break;
                default:
                    $(".baseline-aggregate[unit='10_MINUTES']").addClass("uk-hidden");
                    $(".baseline-aggregate[unit='HOURS']").removeClass("uk-hidden");
                    $(".baseline-aggregate[unit='DAYS']").removeClass("uk-hidden");
                    $(".granularity-btn-group").removeClass("vertical");
            }
        }


        //Check if all form components are populated
        window.responseDataPopulated++
        formComponentPopulated()

    })
}