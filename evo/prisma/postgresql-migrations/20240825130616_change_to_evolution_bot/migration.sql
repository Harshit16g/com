/*
  Warnings:

  - You are about to drop the `GenericBot` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `GenericSetting` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "GenericBot" DROP CONSTRAINT "GenericBot_instanceId_fkey";

-- DropForeignKey
ALTER TABLE "GenericSetting" DROP CONSTRAINT "GenericSetting_botIdFallback_fkey";

-- DropForeignKey
ALTER TABLE "GenericSetting" DROP CONSTRAINT "GenericSetting_instanceId_fkey";

-- DropTable
DROP TABLE "GenericBot";

-- DropTable
DROP TABLE "GenericSetting";

-- CreateTable
CREATE TABLE "evoBot" (
    "id" TEXT NOT NULL,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "description" VARCHAR(255),
    "apiUrl" VARCHAR(255),
    "apiKey" VARCHAR(255),
    "expire" INTEGER DEFAULT 0,
    "keywordFinish" VARCHAR(100),
    "delayMessage" INTEGER,
    "unknownMessage" VARCHAR(100),
    "listeningFromMe" BOOLEAN DEFAULT false,
    "stopBotFromMe" BOOLEAN DEFAULT false,
    "keepOpen" BOOLEAN DEFAULT false,
    "debounceTime" INTEGER,
    "ignoreJids" JSONB,
    "triggerType" "TriggerType",
    "triggerOperator" "TriggerOperator",
    "triggerValue" TEXT,
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP NOT NULL,
    "instanceId" TEXT NOT NULL,

    CONSTRAINT "evoBot_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "evoBotSetting" (
    "id" TEXT NOT NULL,
    "expire" INTEGER DEFAULT 0,
    "keywordFinish" VARCHAR(100),
    "delayMessage" INTEGER,
    "unknownMessage" VARCHAR(100),
    "listeningFromMe" BOOLEAN DEFAULT false,
    "stopBotFromMe" BOOLEAN DEFAULT false,
    "keepOpen" BOOLEAN DEFAULT false,
    "debounceTime" INTEGER,
    "ignoreJids" JSONB,
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP NOT NULL,
    "botIdFallback" VARCHAR(100),
    "instanceId" TEXT NOT NULL,

    CONSTRAINT "evoBotSetting_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "evoBotSetting_instanceId_key" ON "evoBotSetting"("instanceId");

-- AddForeignKey
ALTER TABLE "evoBot" ADD CONSTRAINT "evoBot_instanceId_fkey" FOREIGN KEY ("instanceId") REFERENCES "Instance"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "evoBotSetting" ADD CONSTRAINT "evoBotSetting_botIdFallback_fkey" FOREIGN KEY ("botIdFallback") REFERENCES "evoBot"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "evoBotSetting" ADD CONSTRAINT "evoBotSetting_instanceId_fkey" FOREIGN KEY ("instanceId") REFERENCES "Instance"("id") ON DELETE CASCADE ON UPDATE CASCADE;
