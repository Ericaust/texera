import {Component, ViewChild, OnInit} from '@angular/core';
import { Response, Http } from '@angular/http';
import 'rxjs/add/operator/toPromise';

import { CurrentDataService } from '../services/current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';
import {TableMetadata} from "../services/table-metadata";

declare var jQuery: any;
declare var Backbone: any;

declare var PrettyJSON: any;

@Component({
  moduleId: module.id,
  selector: 'side-bar-container',
  templateUrl: './side-bar.component.html',
  styleUrls: ['../style.css']
})

export class SideBarComponent {
  data: any;
  attributes: string[] = [];
  currentResultTimeStamp: string = "";

  operatorId: number;
  operatorTitle: string;

  hiddenList: string[] = ["operatorType", "luceneAnalyzer", "matchingType"];

  selectorList: string[] = ["matchingType", "nlpEntityType", "splitType", "sampleType", "comparisonType", "aggregationType", "attributes", "tableName", "attribute"].concat(this.hiddenList);
  
  matcherList: string[] = ["conjunction", "phrase", "substring"];
  nlpEntityList: string[] = ["noun", "verb", "adjective", "adverb", "ne_all", "number", "location", "person", "organization", "money", "percent", "date", "time"];
  regexSplitList: string[] = ["left", "right", "standalone"];
  samplerList: string[] = ["random", "firstk"];

  compareList: string[] = ["=", ">", ">=", "<", "<=", "≠"];
  aggregationList: string[] = ["min", "max", "count", "sum", "average"];

  attributeItems:Array<string> = [];
  tableNameItems:Array<string> = [];
  selectedAttributesList:Array<string> = [];
  selectedAttribute:string = "";
  metadataList:Array<TableMetadata> = [];

  @ViewChild('MyModal')
  modal: ModalComponent;

  ModalOpen() {
    this.modal.open();
  }
  ModalClose() {
    this.modal.close();
  }
  
  downloadExcel() {
    if (this.currentResultTimeStamp === "") {
      console.log("currentResultTimeStamp is empty")
    } else {
      console.log("proceed to http request")
      let downloadUrl = "http://textdb.ics.uci.edu:1200/api/download/" + this.currentResultTimeStamp;
      console.log(downloadUrl)
      this.http.get(downloadUrl).toPromise().then(function(data) {
        window.location.href = downloadUrl;
      });
    }
  }

  checkInHidden(name: string) {
    return jQuery.inArray(name, this.hiddenList);
  }
  checkInSelector(name: string) {
    return jQuery.inArray(name, this.selectorList);
  }

  constructor(private currentDataService: CurrentDataService, private http: Http) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.data = data.operatorData;
        this.operatorId = data.operatorNum;
        this.operatorTitle = data.operatorData.properties.title;
        this.attributes = [];
        for (var attribute in data.operatorData.properties.attributes) {
          this.attributes.push(attribute);
        }

        // initialize selected attributes
        this.selectedAttribute = "";

        // and load previously saved attributes and proper attributes for the selected table
        this.selectedAttributesList = data.operatorData.properties.attributes.attributes;
        this.getAttributesForTable(data.operatorData.properties.attributes.tableName);
      });

    currentDataService.checkPressed$.subscribe(
      data => {
        jQuery.hideLoading();
        console.log(data);
        if (data.code === 0) {
          var jsonMessage = JSON.parse(data.message);
          this.currentResultTimeStamp = jsonMessage.timeStamp;


          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: jsonMessage.results
          });
        } else {
          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: {"message": data.message}
          });
        }

        this.ModalOpen();

      });

    currentDataService.metadataRetrieved$.subscribe(
      data => {
        this.metadataList = data;
        let metadata: (Array<TableMetadata>) = data;
        metadata.forEach(x => {
          this.tableNameItems.push((x.tableName));
        });
      }
    )
  }

  humanize(name: string): string {
    // TODO: rewrite this function to handle camel case
    // e.g.: humanize camelCase -> camel case
    var frags = name.split('_');
    for (var i = 0; i < frags.length; i++) {
      frags[i] = frags[i].charAt(0).toUpperCase() + frags[i].slice(1);
    }
    return frags.join(' ');
  }

  onFormChange (attribute: string) {
    jQuery("#the-flowchart").flowchart("setOperatorData", this.operatorId, this.data);
  }

  onDelete() {
    this.operatorTitle = "Operator";
    this.attributes = [];
    jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
    this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
  }

  attributeSelected () {
    this.selectedAttributesList.push(this.selectedAttribute);
    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.onFormChange("attributes");
  }

  manuallyAdded (event:string) {
    if (event.length === 0) {
      // removed all attributes
      this.selectedAttributesList = [];
    } else {
      this.selectedAttributesList = event.split(",");
    }

    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.onFormChange("attributes");
  }

  getAttributesForTable (event:string) {
    this.attributeItems = [];

    this.metadataList.forEach(x => {
      if (x.tableName === event) {
        x.attributes.forEach(
          y => {
            if (!y.attributeName.startsWith("_")) {
              this.attributeItems.push(y.attributeName);
            }
          });
      }
    });

    this.onFormChange("tableName");
  }

}
