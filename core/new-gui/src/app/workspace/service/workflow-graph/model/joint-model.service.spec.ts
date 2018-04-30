import { TestBed, inject } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';
import { isEqual } from 'lodash';

import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { JointModelService } from './joint-model.service';
import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';

import { getMockScanPredicate, getMockResultPredicate, getMockScanResultLink, getMockPoint } from './mock-workflow-data';
import { Point } from './../../../types/common.interface';


describe('JointModelService', () => {

  /**
   * Gets the JointJS graph object <joint.dia.Grap>) from JointModelSerivce
   * @param jointModelService
   */
  function getJointGraph(jointModelService: JointModelService): joint.dia.Graph {
    // we don't want to expose the jointGraph to be public accessible,
    //   but we need to access it in the test cases,
    //   therefore we cast it to <any> type to bypass the private constraint
    // if the jointGraph object is changed, this needs to be changed as well
    return (jointModelService as any).jointGraph;
  }

  describe('should react to events from workflow action', () => {

    beforeEach(() => {
      TestBed.configureTestingModule({
        providers: [
          JointModelService,
          WorkflowActionService,
          JointUIService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ]
      });
      // do not initialize the services in beforeEach
      // because we need to spy on them in each test case
    });

    it('should be created', inject([JointModelService], (service: JointModelService) => {
      expect(service).toBeTruthy();
    }));

    it('should add an operator element when add operator is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: getMockScanPredicate(), point: getMockPoint() } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onAddOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID)).toBeTruthy();
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID).isElement()).toBeTruthy();
        }
      });

    }));

    it('should emit operator delete event correctly when delete operator is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: getMockScanPredicate(), point: getMockPoint() } })
      );

      spyOn(workflowActionService, '_onDeleteOperatorAction').and.returnValue(
        m.hot('--d-|', { d: { operatorID: getMockScanPredicate().operatorID } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onDeleteOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCells().length).toEqual(0);
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID)).toBeFalsy();
        }
      });

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('--e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

    }));


    it('should add an operator link when add operator is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-b-|', {
          a : { operator: getMockScanPredicate(), point: getMockPoint()  },
          b : { operator: getMockResultPredicate(), point: getMockPoint() }
        })
      );

      spyOn(workflowActionService, '_onAddLinkAction').and.returnValue(
        m.hot('----c-|', {
          c : { link : getMockScanResultLink() }
        })
      );

      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onAddLinkAction().subscribe({
        complete : () => {
          expect(getJointGraph(jointModelService).getLinks().length).toEqual(1);
          expect(jointModelService.getJointLinkCell(getMockScanResultLink())).toBeTruthy();
        }
      });
    }));

    it('should emit link delete event correctly when delete link is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-b-|', {
          a : { operator: getMockScanPredicate(), point: getMockPoint()  },
          b : { operator: getMockResultPredicate(), point: getMockPoint() }
        })
      );

      spyOn(workflowActionService, '_onAddLinkAction').and.returnValue(
        m.hot('----c-|', {
          c : { link : getMockScanResultLink() }
        })
      );

      spyOn(workflowActionService, '_onDeleteLinkAction').and.returnValue(
        m.hot('------d-|', {
          d : { link: getMockScanResultLink() }
        })
      );

      const jointModelService = TestBed.get(JointModelService);
      workflowActionService._onDeleteLinkAction().subscribe({
        complete : () => {
          expect(getJointGraph(jointModelService).getLinks().length).toEqual(0);
          expect(getJointGraph(jointModelService).getCells().length).toEqual(2);
          expect(
            () => jointModelService.getJointLinkCell(getMockScanResultLink())
          ).toThrowError(new RegExp('no link is found'));
        }
      });


      const jointLinkDeleteStream = jointModelService.onJointLinkCellDelete().map((value: any) => 'e');
      const expectedStream = m.hot('------e-');
      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

    }));

  });

  describe('should react to events from JointJS user actions from UI', () => {

    let workflowActionService: WorkflowActionService;
    let jointModelService: JointModelService;

    beforeEach(() => {
      TestBed.configureTestingModule({
        providers: [
          JointModelService,
          WorkflowActionService,
          JointUIService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ]
      });

      workflowActionService = TestBed.get(WorkflowActionService);
      jointModelService = TestBed.get(JointModelService);

    });

    it('should be created', inject([JointModelService], (service: JointModelService) => {
      expect(service).toBeTruthy();
    }));

    it('should emit operator delete event correctly when operator is deleted by JointJS', marbles((m) => {

      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());

      m.hot('-e-').do(v => getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('-e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

    }));

    it('should emit link add event correctly when link is added by JointJS', marbles((m) => {
      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
      workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());

      const jointUIService = TestBed.get(JointUIService);
      const jointLinkCell = jointUIService.getJointjsLinkElement(getMockScanResultLink().source, getMockScanResultLink().target);


      m.hot('-e-').do(v => getJointGraph(jointModelService).addCell(jointLinkCell)).subscribe();

      const jointLinkAddStream = jointModelService.onJointLinkCellAdd().map(value => 'e');
      const expectedStream = m.hot('-e-');
      m.expect(jointLinkAddStream).toBeObservable(expectedStream);


    }));

    it('should emit link delete event correctly when link is deleted by JointJS', marbles((m) => {

      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
      workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());

      const jointUIService = TestBed.get(JointUIService);
      const jointLinkCell = jointUIService.getJointjsLinkElement(getMockScanResultLink().source, getMockScanResultLink().target);
      getJointGraph(jointModelService).addCell(jointLinkCell);

      m.hot('---e-').do(v => jointModelService.getJointLinkCell(getMockScanResultLink()).remove()).subscribe();

      const jointLinkDeleteStream = jointModelService.onJointLinkCellDelete().map(value => 'e');
      const expectedStream = m.hot('---e-');
      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

    }));

  });


});

