import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { Logger } from '@config/logger.config';
import { evoBot, IntegrationSession } from '@prisma/client';

import { BaseChatbotController } from '../../base-chatbot.controller';
import { evoBotDto } from '../dto/evoBot.dto';
import { evoBotService } from '../services/evoBot.service';

export class evoBotController extends BaseChatbotController<evoBot, evoBotDto> {
  constructor(
    private readonly evoBotService: evoBotService,
    prismaRepository: PrismaRepository,
    waMonitor: WAMonitoringService,
  ) {
    super(prismaRepository, waMonitor);

    this.botRepository = this.prismaRepository.evoBot;
    this.settingsRepository = this.prismaRepository.evoBotSetting;
    this.sessionRepository = this.prismaRepository.integrationSession;
  }

  public readonly logger = new Logger('evoBotController');
  protected readonly integrationName = 'evoBot';

  integrationEnabled = true; // Set to true by default or use config value if available
  botRepository: any;
  settingsRepository: any;
  sessionRepository: any;
  userMessageDebounce: { [key: string]: { message: string; timeoutId: NodeJS.Timeout } } = {};

  // Implementation of abstract methods required by BaseChatbotController

  protected getFallbackBotId(settings: any): string | undefined {
    return settings?.botIdFallback;
  }

  protected getFallbackFieldName(): string {
    return 'botIdFallback';
  }

  protected getIntegrationType(): string {
    return 'evo';
  }

  protected getAdditionalBotData(data: evoBotDto): Record<string, any> {
    return {
      apiUrl: data.apiUrl,
      apiKey: data.apiKey,
    };
  }

  // Implementation for bot-specific updates
  protected getAdditionalUpdateFields(data: evoBotDto): Record<string, any> {
    return {
      apiUrl: data.apiUrl,
      apiKey: data.apiKey,
    };
  }

  // Implementation for bot-specific duplicate validation on update
  protected async validateNoDuplicatesOnUpdate(
    botId: string,
    instanceId: string,
    data: evoBotDto,
  ): Promise<void> {
    const checkDuplicate = await this.botRepository.findFirst({
      where: {
        id: {
          not: botId,
        },
        instanceId: instanceId,
        apiUrl: data.apiUrl,
        apiKey: data.apiKey,
      },
    });

    if (checkDuplicate) {
      throw new Error('evo Bot already exists');
    }
  }

  // Process bot-specific logic
  protected async processBot(
    instance: any,
    remoteJid: string,
    bot: evoBot,
    session: IntegrationSession,
    settings: any,
    content: string,
    pushName?: string,
    msg?: any,
  ) {
    await this.evoBotService.process(instance, remoteJid, bot, session, settings, content, pushName, msg);
  }
}
