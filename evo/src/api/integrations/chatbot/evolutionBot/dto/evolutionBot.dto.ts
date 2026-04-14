import { BaseChatbotDto, BaseChatbotSettingDto } from '../../base-chatbot.dto';

export class evoBotDto extends BaseChatbotDto {
  apiUrl: string;
  apiKey: string;
}

export class evoBotSettingDto extends BaseChatbotSettingDto {
  botIdFallback?: string;
}
