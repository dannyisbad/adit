// Mirrors daemon C# models — field names use camelCase (System.Text.Json default)

export interface BluetoothEndpointRecord {
  transport: string;
  id: string;
  name: string;
  isPaired: boolean;
  aepAddress?: string | null;
  bluetoothAddress?: string | null;
  isConnected?: boolean | null;
  isPresent?: boolean | null;
  containerId?: string | null;
  category?: string | null;
}

export interface SessionStateChangedRecord {
  transport: string;
  phase: string | number; // daemon currently sends DeviceSessionPhase as an enum value
  timestampUtc: string;
  detail?: string | null;
  error?: string | null;
}

export interface DaemonRuntimeSnapshot {
  phase: string;
  lastReason: string;
  lastAttemptUtc?: string | null;
  lastSuccessfulSyncUtc?: string | null;
  lastContactsRefreshUtc?: string | null;
  lastError?: string | null;
  consecutiveFailures: number;
  contactCount: number;
  messageCount: number;
  notificationCount: number;
  conversationCount: number;
  autoEvictPhoneLink: boolean;
  notificationsMode: string;
  notificationsEnabled: boolean;
  target?: BluetoothEndpointRecord | null;
  mapSession?: SessionStateChangedRecord | null;
  ancsSession?: SessionStateChangedRecord | null;
}

export interface CapabilityStateRecord {
  state: string;
  stability: string;
  enabled: boolean;
  reason?: string | null;
  recommendedAction?: string | null;
  recommendedBootstrap?: string | null;
  detail?: string | null;
}

export interface DaemonCapabilitiesSnapshot {
  messaging: CapabilityStateRecord;
  contacts: CapabilityStateRecord;
  notifications: CapabilityStateRecord;
}

export interface NotificationsBootstrapSnapshot {
  state: string;
  mode: string;
  enabled: boolean;
  canAttemptEnable: boolean;
  recommendedFlow: string;
  reason?: string | null;
  detail?: string | null;
}

export interface SupportedSetupSnapshot {
  supportedFlow: string;
  state: string;
  experimentalPairingApiEnabled: boolean;
  recommendedAction: string;
  reason?: string | null;
}

export interface DoctorSnapshot {
  overall: string;
  summary: string;
  nextSteps: string[];
  capabilities: DaemonCapabilitiesSnapshot;
  notificationsBootstrap: NotificationsBootstrapSnapshot;
  setup: SupportedSetupSnapshot;
}

export interface ThreadChooserHeadSnapshot {
  structDim: number;
  semanticScalarDim: number;
  semanticVecDim: number;
  hiddenDim: number;
  candidateEncoderLayers: number;
  candidateEncoderHeads: number;
  semanticResidualLogit: boolean;
}

export interface ThreadChooserCandidateScoreSnapshot {
  index: number;
  threadId: string;
  isGroup: boolean;
  logit: number;
  probability: number;
  semantic: Record<string, number>;
}

export interface ThreadChooserLastScoreSnapshot {
  timestampUtc: string;
  sampleId: string;
  candidateCount: number;
  predictedIndex: number;
  predictedThreadId: string;
  scores: number[];
  probabilities: number[];
  candidates: ThreadChooserCandidateScoreSnapshot[];
}

export interface ThreadChooserLastRunSnapshot {
  timestampUtc: string;
  totalMessages: number;
  scoredSamples: number;
  rerankedMessages: number;
  lastScore?: ThreadChooserLastScoreSnapshot | null;
}

export interface ThreadChooserStatusSnapshot {
  enabled: boolean;
  status: string;
  reason?: string | null;
  scriptExists: boolean;
  checkpointExists: boolean;
  pythonPath: string;
  sidecarRunning: boolean;
  serviceHealthy: boolean;
  processId?: number | null;
  serviceUrl: string;
  scriptPath: string;
  checkpointPath: string;
  configuredModelName: string;
  port: number;
  maxCandidates: number;
  historyTurns: number;
  resolvedModelName?: string | null;
  semanticCachePath?: string | null;
  device?: string | null;
  dtype?: string | null;
  includeCandidateScore?: boolean | null;
  includeCandidateDisplayNameInQwen?: boolean | null;
  head?: ThreadChooserHeadSnapshot | null;
  lastHealthCheckUtc?: string | null;
  lastError?: string | null;
  lastRun?: ThreadChooserLastRunSnapshot | null;
}

export interface ContactPhoneRecord {
  raw: string;
  normalized?: string | null;
  type: string;
}

export interface ContactRecord {
  uniqueIdentifier?: string | null;
  displayName: string;
  phones: ContactPhoneRecord[];
  emails: string[];
}

export interface MessageParticipantRecord {
  name: string;
  phones: string[];
  emails: string[];
}

export interface MessageRecord {
  folder: string;
  handle?: string | null;
  type?: string | null;
  subject?: string | null;
  datetime?: string | null;
  senderName?: string | null;
  senderAddressing?: string | null;
  recipientAddressing?: string | null;
  size?: number | null;
  attachmentSize?: number | null;
  priority?: string | null;
  read?: boolean | null;
  sent?: boolean | null;
  protected?: boolean | null;
  body?: string | null;
  messageType?: string | null;
  status?: string | null;
  originators: MessageParticipantRecord[];
  recipients: MessageParticipantRecord[];
}

export interface ConversationParticipantRecord {
  key: string;
  displayName: string;
  phones: string[];
  emails: string[];
  isSelf: boolean;
}

export interface ConversationSnapshot {
  conversationId: string;
  displayName: string;
  isGroup: boolean;
  lastMessageUtc?: string | null;
  messageCount: number;
  unreadCount: number;
  lastPreview?: string | null;
  participants: ConversationParticipantRecord[];
  sourceFolders: string[];
  lastSenderDisplayName?: string | null;
}

export interface SynthesizedMessageRecord {
  messageKey: string;
  conversationId: string;
  conversationDisplayName: string;
  isGroup: boolean;
  sortTimestampUtc?: string | null;
  participants: ConversationParticipantRecord[];
  message: MessageRecord;
}

export interface DaemonEventRecord {
  sequence: number;
  timestampUtc: string;
  type: string;
  payload?: unknown;
}

export interface NotificationRecord {
  notificationUid: number;
  eventKind: number; // 0=Added, 1=Modified, 2=Removed
  eventFlags: number;
  category: number; // 0=Other, 1=IncomingCall, 2=MissedCall, 3=VoiceMail, 4=Social, 5=Schedule, 6=Email, 7=News, 8=HealthAndFitness, 9=BusinessAndFinance, 10=Location, 11=Entertainment
  categoryCount: number;
  receivedAtUtc: string;
  appIdentifier?: string | null;
  title?: string | null;
  subtitle?: string | null;
  message?: string | null;
  messageSize?: string | null;
  date?: string | null;
  positiveActionLabel?: string | null;
  negativeActionLabel?: string | null;
  attributes: Record<string, string>;
}

export interface StoredNotificationRecord {
  deviceId: string;
  isActive: boolean;
  updatedAtUtc: string;
  removedAtUtc?: string | null;
  notification: NotificationRecord;
}

export interface NotificationsResponse {
  target: BluetoothEndpointRecord;
  count: number;
  notifications: StoredNotificationRecord[];
}

export const NotificationCategoryLabels: Record<number, string> = {
  0: "Other",
  1: "Incoming Call",
  2: "Missed Call",
  3: "Voicemail",
  4: "Social",
  5: "Schedule",
  6: "Email",
  7: "News",
  8: "Health & Fitness",
  9: "Business & Finance",
  10: "Location",
  11: "Entertainment",
};

// API response wrappers

export interface StatusResponse {
  runtime: DaemonRuntimeSnapshot;
  setup: SupportedSetupSnapshot;
  capabilities: DaemonCapabilitiesSnapshot;
  notificationsSetup: NotificationsBootstrapSnapshot;
  notificationsBootstrap: NotificationsBootstrapSnapshot;
}

export interface DevicesResponse {
  leDevices: Array<{
    id: string;
    name: string;
    isPaired: boolean;
    address?: string | null;
    isConnected?: boolean | null;
    isPresent?: boolean | null;
    containerId?: string | null;
  }>;
  endpoints: BluetoothEndpointRecord[];
}

export interface ConversationsResponse {
  target: BluetoothEndpointRecord;
  count: number;
  conversations: ConversationSnapshot[];
}

export interface ConversationMessagesResponse {
  target: BluetoothEndpointRecord;
  conversationId: string;
  count: number;
  messages: SynthesizedMessageRecord[];
}

export interface ContactsResponse {
  target: BluetoothEndpointRecord;
  count: number;
  contacts: ContactRecord[];
}

export interface ContactSearchResponse {
  target: BluetoothEndpointRecord;
  count: number;
  contacts: ContactRecord[];
}

export interface SendMessageResponse {
  sendIntentId: string;
  target: BluetoothEndpointRecord;
  recipient: string;
  resolvedContact?: ContactRecord | null;
  result: {
    isSuccess: boolean;
    responseCode?: string | null;
    messageHandle?: string | null;
  };
}

export interface DoctorResponse {
  runtime: DaemonRuntimeSnapshot;
  setup: SupportedSetupSnapshot;
  doctor: DoctorSnapshot;
}

export interface RecentEventsResponse {
  count: number;
  events: DaemonEventRecord[];
}

export interface CapabilitiesResponse {
  runtime: DaemonRuntimeSnapshot;
  setup: SupportedSetupSnapshot;
  capabilities: DaemonCapabilitiesSnapshot;
  notificationsSetup: NotificationsBootstrapSnapshot;
  notificationsBootstrap: NotificationsBootstrapSnapshot;
}
