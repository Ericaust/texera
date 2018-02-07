import { Component, OnInit, ViewChild } from '@angular/core';

import { NavigationComponent } from './navigation/navigation.component';
import { OperatorViewComponent } from './operator-view/operator-view.component';
import { WorkflowEditorComponent } from './workflow-editor/workflow-editor.component';
import { ResultViewComponent } from './result-view/result-view.component';
import { PropertyEditorComponent } from './property-editor/property-editor.component';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { ExecuteWorkflowService } from '../service/execute-workflow/execute-workflow.service';
import { OperatorDragDropService } from '../service/operator-drag-drop/operator-drag-drop.service';
import { OperatorUIElementService } from '../service/operator-ui-element/operator-ui-element.service';
import { WorkflowModelService } from '../service/workflow-graph/workflow-model.service';
import { WorkflowDataChangeService } from '../service/workflow-graph/workflow-data-change.service';
import { WorkflowUIChangeService } from '../service/workflow-graph/workflow-ui-change.service';

@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    OperatorMetadataService,
    WorkflowModelService,
    WorkflowUIChangeService,
    WorkflowDataChangeService,
    ExecuteWorkflowService,
    OperatorDragDropService,
    OperatorUIElementService
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
