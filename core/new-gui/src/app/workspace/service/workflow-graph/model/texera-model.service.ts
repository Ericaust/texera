import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from './workflow-action.service';
import { JointModelService } from './joint-model.service';

import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';
import { OperatorSchema } from './../../../types/operator-schema';
import { WorkflowGraph, OperatorLink, OperatorPredicate } from './../../../types/workflow-graph';
import { OperatorPort } from '../../../types/operator-port';

/**
 *
 */
@Injectable()
export class TexeraModelService {

  private texeraGraph: WorkflowGraph;

  private addOperatorSubject = new Subject<OperatorPredicate>();
  private deleteOperatorSubject = new Subject<OperatorPredicate>();
  private addLinkSubject = new Subject<OperatorLink>();
  private deleteLinkSubject = new Subject<OperatorLink>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointModelService: JointModelService,
  ) {
    // bypass Typescript type system to access a private variable
    //   because Typescript doesn't support package (same folder) access level :(
    //   and we don't want to expose the write-able workflow graph to be public
    // this is very dangerous and should be prohibited in most cases
    this.texeraGraph = (workflowActionService as any).texeraGraph;


    this.workflowActionService._onAddOperatorAction()
      .subscribe(value => this.addOperator(value.operator));

    this.jointModelService.onJointOperatorCellDelete()
      .map(element => element.id.toString())
      .subscribe(elementID => this.deleteOperator(elementID));

    this.jointModelService.onJointLinkCellAdd()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

    this.jointModelService.onJointLinkCellDelete()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.deleteLink(link.source, link.target));

    const jointLinkChange = this.jointModelService.onJointLinkCellChange()
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain

      .do(link => {
        if (this.texeraGraph.hasLinkWithID(link.id.toString())) {
          this.deleteLinkWithID(link.id.toString());
        }
      })
      // .do((link) => {
      //   const texeraOperatorLink = TexeraModelService.getOperatorLink(link);
      //   if (this.texeraGraph.hasLink(texeraOperatorLink.source, texeraOperatorLink.target)) {
      //     this.deleteLink(texeraOperatorLink.source, texeraOperatorLink.target);
      //   }
      // })
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => {
        this.addLink(link);
      })
      ;

  }

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  public onOperatorAdd(): Observable<OperatorPredicate> {
    return this.addOperatorSubject.asObservable();
  }

  public onOperatorDelete(): Observable<OperatorPredicate> {
    return this.deleteOperatorSubject.asObservable();
  }

  public onLinkAdd(): Observable<OperatorLink> {
    return this.addLinkSubject.asObservable();
  }

  public onLinkDelete(): Observable<OperatorLink> {
    return this.deleteLinkSubject.asObservable();
  }

  private addOperator(operator: OperatorPredicate): void {
    this.texeraGraph.addOperator(operator);
    this.addOperatorSubject.next(operator);
  }

  private deleteOperator(operatorID: string): void {
    const deletedOperator = this.texeraGraph.deleteOperator(operatorID);
    this.deleteOperatorSubject.next(deletedOperator);
  }

  private addLink(link: OperatorLink): void {
    this.texeraGraph.addLink(link);
    this.addLinkSubject.next(link);
  }

  private deleteLink(source: OperatorPort, target: OperatorPort): void {
    const deletedLink = this.texeraGraph.deleteLink(source, target);
    this.deleteLinkSubject.next(deletedLink);
  }

  private deleteLinkWithID(linkID: string): void {
    const deletedLink = this.texeraGraph.deleteLinkWithID(linkID);
    this.deleteLinkSubject.next(deletedLink);
  }

  /**
   * Transforms a JointJS link (joint.dia.Link) to a Texera Link Object
   * The JointJS link must be valid, otherwise an error will be thrown.
   * @param jointLink
   */
  static getOperatorLink(jointLink: joint.dia.Link): OperatorLink {

    // the link should be a valid link (both source and target are connected to an operator)
    // isValidLink function is not reused because of Typescript strict null checking

    const SourceID = jointLink.attributes.source.id;
    const TargetID = jointLink.attributes.target.id;

    if (SourceID === undefined || TargetID === undefined) {
      throw new Error('Texera-model.service: getOperatorLink has invalid JointJS Link:');
    }

    return {
      linkID: jointLink.id.toString(),
      source: {
        operatorID: SourceID.toString(),
        portID: jointLink.get('source').port.toString()
      },
      target: {
        operatorID: TargetID.toString(),
        portID: jointLink.get('target').port.toString()
      }
    };
  }

  /**
   * Determines if a jointJS link is valid (both ends are connected to a port of an operator).
   * If a JointJS link's target is still a point (not connected), it's not a valid link.
   * @param jointLink
   */
  static isValidLink(jointLink: joint.dia.Link): boolean {
    return jointLink.attributes.source.id !== undefined && jointLink.attributes.target.id !== undefined;
  }


}


