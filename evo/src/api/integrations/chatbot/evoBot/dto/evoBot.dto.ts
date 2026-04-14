import { BaseChatbotDto, BaseChatbotSettingDto } from '../../base-chatbot.dto';

export class EvoBotDto extends BaseChatbotDto {
  apiUrl: string;
  apiKey: string;
}

export class EvoBotSettingDto extends BaseChatbotSettingDto {
  botIdFallback?: string;
}
