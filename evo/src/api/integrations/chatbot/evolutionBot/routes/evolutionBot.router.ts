import { RouterBroker } from '@api/abstract/abstract.router';
import { IgnoreJidDto } from '@api/dto/chatbot.dto';
import { InstanceDto } from '@api/dto/instance.dto';
import { HttpStatus } from '@api/routes/index.router';
import { evoBotController } from '@api/server.module';
import { instanceSchema } from '@validate/instance.schema';
import { RequestHandler, Router } from 'express';

import { evoBotDto, evoBotSettingDto } from '../dto/evoBot.dto';
import {
  evoBotIgnoreJidSchema,
  evoBotSchema,
  evoBotSettingSchema,
  evoBotStatusSchema,
} from '../validate/evoBot.schema';

export class evoBotRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();
    this.router
      .post(this.routerPath('create'), ...guards, async (req, res) => {
        const response = await this.dataValidate<evoBotDto>({
          request: req,
          schema: evoBotSchema,
          ClassRef: evoBotDto,
          execute: (instance, data) => evoBotController.createBot(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get(this.routerPath('find'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => evoBotController.findBot(instance),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .get(this.routerPath('fetch/:evoBotId'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => evoBotController.fetchBot(instance, req.params.evoBotId),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .put(this.routerPath('update/:evoBotId'), ...guards, async (req, res) => {
        const response = await this.dataValidate<evoBotDto>({
          request: req,
          schema: evoBotSchema,
          ClassRef: evoBotDto,
          execute: (instance, data) => evoBotController.updateBot(instance, req.params.evoBotId, data),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .delete(this.routerPath('delete/:evoBotId'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => evoBotController.deleteBot(instance, req.params.evoBotId),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('settings'), ...guards, async (req, res) => {
        const response = await this.dataValidate<evoBotSettingDto>({
          request: req,
          schema: evoBotSettingSchema,
          ClassRef: evoBotSettingDto,
          execute: (instance, data) => evoBotController.settings(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .get(this.routerPath('fetchSettings'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => evoBotController.fetchSettings(instance),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('changeStatus'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: evoBotStatusSchema,
          ClassRef: InstanceDto,
          execute: (instance, data) => evoBotController.changeStatus(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .get(this.routerPath('fetchSessions/:evoBotId'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => evoBotController.fetchSessions(instance, req.params.evoBotId),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('ignoreJid'), ...guards, async (req, res) => {
        const response = await this.dataValidate<IgnoreJidDto>({
          request: req,
          schema: evoBotIgnoreJidSchema,
          ClassRef: IgnoreJidDto,
          execute: (instance, data) => evoBotController.ignoreJid(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      });
  }

  public readonly router: Router = Router();
}
