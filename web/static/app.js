const $ = (id) => document.getElementById(id);
const translations = {
  en: {
    pageTitle: 'Norma · Resident Chat', createGroup: 'Create group', incognitoHint: 'New chats stay out of local history and long-term memory',
    incognitoMode: 'Incognito mode', newChats: 'New chats', onlineModels: 'Online models', groups: 'Groups', groupChats: 'Group chats',
    settings: 'Settings', openChatList: 'Open chat list', archive: 'Archive', welcomeTitle: 'Move ideas forward through conversation.',
    welcomeDescription: "Choose an online model on the left to start a private chat. For collaboration, create a group and invite different LLM Residents to build on each other's work.",
    chatWithModel: 'Chat with an online model', soloCapability: 'Private chat · focused work', groupCapability: 'Group chat · multi-agent collaboration',
    conversation: 'Conversation', messagePlaceholder: 'Message a Resident…', message: 'Message', attachHint: 'Add an image or GIF',
    imageGif: 'Image / GIF', keyboardHint: 'Enter to send · Shift + Enter for a new line', sendMessage: 'Send message',
    close: 'Close', createGroupDescription: 'Choose at least two different online LLM Residents to collaborate.',
    roomName: 'Chat name', roomNameExample: 'For example: Discuss a new feature', chooseResidents: 'Choose LLM Residents',
    closeSettings: 'Close settings', settingsNavigation: 'Settings navigation', preferences: 'Preferences', general: 'General',
    chats: 'Chats', archivedChats: 'Archived chats', searchArchived: 'Search archived chats', chatType: 'Chat type',
    allChats: 'All chats', privateChats: 'Private chats', modelFilter: 'Model filter', allModels: 'All models',
    settingsDescription: 'Preferences for this local service.', language: 'Language', languageDescription: 'Choose the language used by this page.',
    pageLanguage: 'Page language', longTermMemory: 'Long-term memory',
    memoryDescription: 'When enabled, memory is extracted when a conversation becomes idle or its context is compacted.',
    connecting: 'Connecting to Residents…', residentCount: '{count} LLM Residents online', online: 'Online',
    noResidents: 'No LLM Residents online', saved: 'Saved', incognito: 'Incognito', incognitoGroup: 'Incognito group · current run only',
    groupMeta: '{models} models · {messages} messages', working: 'Working', noGroups: 'No group chats yet',
    chatTypeValue: 'Chat type: {value}', modelFilterValue: 'Model filter: {value}', chatCount: '{count} chats',
    noMessages: 'No messages yet', modelCount: '{count} models', messageCount: '{count} messages',
    unarchive: 'Unarchive', unarchiveRoom: 'Unarchive {name}', noMatchingArchived: 'No archived chats match your search',
    noArchived: 'No archived chats yet', you: 'You', finalAnswer: 'Answer for you', waitingFirstReply: 'Waiting for the first reply',
    workingElapsed: '{member} {status} · {seconds}s elapsed', incognitoConversation: 'Incognito chat',
    soloTask: 'Single Agent task', collaboration: 'Multi-Agent collaboration · {count}',
    incognitoNote: 'Incognito chats are not saved to local history and do not create long-term memories.',
    soloNote: 'A Resident remembers the private and group chats it joins. Verify important conclusions.',
    groupNote: 'Use @{member} to ask one model; without a mention, all models collaborate.',
    unarchiveToContinue: 'Unarchive to continue chatting', waitForResident: 'Wait for the Resident to finish…',
    startConversation: 'Start a conversation', chooseOnlineModel: 'Choose an online model',
    archivedToast: 'Archived. You can unarchive it in Settings.', unarchivedToast: 'Chat unarchived',
    memoryStats: '{stored} stored memories · {cached} cached · {hot} hot',
    memoryEnabled: 'Long-term memory enabled', memoryDisabled: 'Long-term memory disabled',
    selectedCount: '{count} selected', chooseTwoDifferent: 'Choose at least 2 different models',
    agentsCollaborating: '{count} Agents collaborating', noLlmResidents: 'No Residents marked as LLM are available. Check Codex sign-in and the server logs.',
    needAnotherResident: 'Only one model is online. Connect a second distinct LLM Resident to create a group.',
    incognitoEnabled: 'New private and group chats will use incognito mode',
    incognitoDisabled: 'New private and group chats will save local history',
    noOnlineResident: 'No LLM Residents are online', supportedImages: 'Use PNG, JPEG, WebP, or GIF images',
    imageSizeLimit: 'Each image must be 8 MiB or smaller', imageCountLimit: 'Up to 4 images per message',
    removeFile: 'Remove {name}', requestFailed: 'Request failed ({status})'
  },
  zh: {
    pageTitle: 'Norma · Resident 聊天', createGroup: '创建群聊', incognitoHint: '新聊天不会保存到本地历史或长期记忆',
    incognitoMode: '无痕模式', newChats: '新聊天', onlineModels: '在线模型', groups: '群聊', groupChats: '群聊',
    settings: '设置', openChatList: '打开会话列表', archive: '归档', welcomeTitle: '让想法，在对话中推进。',
    welcomeDescription: '点击左侧在线模型，直接开始单聊。需要协作时，创建群聊并邀请不同的 LLM Resident 接力分析、相互补充。',
    chatWithModel: '与在线模型对话', soloCapability: '单聊 · 专注执行', groupCapability: '群聊 · 多 Agent 协作',
    conversation: '聊天内容', messagePlaceholder: '发送消息给 Resident…', message: '消息内容', attachHint: '添加图片或 GIF',
    imageGif: '图片 / GIF', keyboardHint: 'Enter 发送 · Shift + Enter 换行', sendMessage: '发送消息',
    close: '关闭', createGroupDescription: '从在线模型中选择至少两个不同的 LLM Resident，组成协作群聊。',
    roomName: '会话名称', roomNameExample: '例如：讨论新功能方案', chooseResidents: '选择 LLM Resident',
    closeSettings: '关闭设置', settingsNavigation: '设置导航', preferences: '偏好', general: '常规',
    chats: '聊天', archivedChats: '已归档的聊天', searchArchived: '搜索已归档聊天', chatType: '聊天类型',
    allChats: '全部聊天', privateChats: '单聊', modelFilter: '模型筛选', allModels: '全部模型',
    settingsDescription: '当前本地服务的偏好设置。', language: '语言', languageDescription: '选择此页面使用的语言。',
    pageLanguage: '页面语言', longTermMemory: '长期记忆',
    memoryDescription: '开启后，仅在会话休眠或上下文压缩时提取记忆。',
    connecting: '正在连接 Resident…', residentCount: '{count} 个 LLM Resident 在线', online: '在线',
    noResidents: '暂无在线 LLM Resident', saved: '有痕', incognito: '无痕', incognitoGroup: '无痕群聊 · 仅当前运行期间',
    groupMeta: '{models} 个模型 · {messages} 条消息', working: '正在处理', noGroups: '还没有群聊',
    chatTypeValue: '聊天类型：{value}', modelFilterValue: '模型筛选：{value}', chatCount: '{count} 个聊天',
    noMessages: '尚无消息', modelCount: '{count} 个模型', messageCount: '{count} 条消息',
    unarchive: '取消归档', unarchiveRoom: '取消归档 {name}', noMatchingArchived: '没有匹配的归档聊天',
    noArchived: '暂无归档聊天', you: '你', finalAnswer: '给你的答复', waitingFirstReply: '首次回复等待中',
    workingElapsed: '{member} {status} · 已等待 {seconds} 秒', incognitoConversation: '无痕对话',
    soloTask: '单 Agent 任务', collaboration: '多 Agent 协作 · {count}',
    incognitoNote: '无痕会话不会保存到本地历史，也不会提取长期记忆。',
    soloNote: '同一个 Resident 会记得它参与的私聊和群聊。重要结论请核实。',
    groupNote: '可用 @{member} 定向提问；不 @ 时全体协作。',
    unarchiveToContinue: '取消归档后可继续对话', waitForResident: '等待 Resident 完成当前任务…',
    startConversation: '开始一次对话', chooseOnlineModel: '选择在线模型',
    archivedToast: '已归档，可在设置中取消归档', unarchivedToast: '已取消归档',
    memoryStats: '原始记忆 {stored} 条 · cache {cached} 条 · hot {hot} 条',
    memoryEnabled: '长期记忆已开启', memoryDisabled: '长期记忆已关闭',
    selectedCount: '已选 {count} 个', chooseTwoDifferent: '至少选择 2 个不同模型',
    agentsCollaborating: '{count} 个 Agent 协作', noLlmResidents: '当前没有标注为 llm 的 Resident。请检查 Codex 登录和服务端启动日志。',
    needAnotherResident: '目前只有一个在线模型。还需接入另一个不同的 LLM Resident 才能创建群聊。',
    incognitoEnabled: '新开的单聊和群聊将使用无痕模式',
    incognitoDisabled: '新开的单聊和群聊将保存本地历史',
    noOnlineResident: '当前没有在线的 LLM Resident', supportedImages: '支持 PNG、JPEG、WebP 和 GIF',
    imageSizeLimit: '单张图片最多 8 MiB', imageCountLimit: '每条消息最多 4 张图片',
    removeFile: '移除 {name}', requestFailed: '请求失败 ({status})'
  }
};
let language = 'en';
try { if (localStorage.getItem('norma-ui-language') === 'zh') language = 'zh'; } catch { /* Storage may be unavailable. */ }
const t = (key, values = {}) => (translations[language][key] || translations.en[key] || key)
  .replace(/\{(\w+)\}/g, (_, name) => values[name] ?? '');
let incognitoMode = false;
try { incognitoMode = localStorage.getItem('norma-incognito-mode') === 'true'; } catch { /* Storage may be unavailable. */ }
const state = { residents: [], rooms: [], selected: null, room: null, events: null, fingerprint: '', files: [], incognitoMode, residentsLoaded: false, memoryStatus: null };
// randomUUID is unavailable on non-secure LAN HTTP origins; getRandomValues still works there.
const clientId = crypto.randomUUID?.()
  ?? Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, '0')).join('');
const markdownRenderer = typeof window.markdownit === 'function'
  ? window.markdownit({ html: false, linkify: false, breaks: false }).disable('image')
  : null;
if (markdownRenderer) {
  markdownRenderer.validateLink = (href) => {
    try { return ['http:', 'https:', 'mailto:'].includes(new URL(href, location.href).protocol); }
    catch { return false; }
  };
}
let heartbeatTimer;
// OpenAI monoblossom from the official OpenAI brand assets.
const GPT_ICON_PATH = 'M304.246 294.611V249.028C304.246 245.189 305.687 242.309 309.044 240.392L400.692 187.612C413.167 180.415 428.042 177.058 443.394 177.058C500.971 177.058 537.44 221.682 537.44 269.182C537.44 272.54 537.44 276.379 536.959 280.218L441.954 224.558C436.197 221.201 430.437 221.201 424.68 224.558L304.246 294.611ZM518.245 472.145V363.224C518.245 356.505 515.364 351.707 509.608 348.349L389.174 278.296L428.519 255.743C431.877 253.826 434.757 253.826 438.115 255.743L529.762 308.523C556.154 323.879 573.905 356.505 573.905 388.171C573.905 424.636 552.315 458.225 518.245 472.141V472.145ZM275.937 376.182L236.592 353.152C233.235 351.235 231.794 348.354 231.794 344.515V238.956C231.794 187.617 271.139 148.749 324.4 148.749C344.555 148.749 363.264 155.468 379.102 167.463L284.578 222.164C278.822 225.521 275.942 230.319 275.942 237.039V376.186L275.937 376.182ZM360.626 425.122L304.246 393.455V326.283L360.626 294.616L417.002 326.283V393.455L360.626 425.122ZM396.852 570.989C376.698 570.989 357.989 564.27 342.151 552.276L436.674 497.574C442.431 494.217 445.311 489.419 445.311 482.699V343.552L485.138 366.582C488.495 368.499 489.936 371.379 489.936 375.219V480.778C489.936 532.117 450.109 570.985 396.852 570.985V570.989ZM283.134 463.99L191.486 411.211C165.094 395.854 147.343 363.229 147.343 331.562C147.343 294.616 169.415 261.509 203.48 247.593V356.991C203.48 363.71 206.361 368.508 212.117 371.866L332.074 441.437L292.729 463.99C289.372 465.907 286.491 465.907 283.134 463.99ZM277.859 542.68C223.639 542.68 183.813 501.895 183.813 451.514C183.813 447.675 184.294 443.836 184.771 439.997L279.295 494.698C285.051 498.056 290.812 498.056 296.568 494.698L417.002 425.127V470.71C417.002 474.549 415.562 477.429 412.204 479.346L320.557 532.126C308.081 539.323 293.206 542.68 277.854 542.68H277.859ZM396.852 599.776C454.911 599.776 503.37 558.513 514.41 503.812C568.149 489.896 602.696 439.515 602.696 388.176C602.696 354.587 588.303 321.962 562.392 298.45C564.791 288.373 566.231 278.296 566.231 268.224C566.231 199.611 510.571 148.267 446.274 148.267C433.322 148.267 420.846 150.184 408.37 154.505C386.775 133.392 357.026 119.958 324.4 119.958C266.342 119.958 217.883 161.22 206.843 215.921C153.104 229.837 118.557 280.218 118.557 331.557C118.557 365.146 132.95 397.771 158.861 421.283C156.462 431.36 155.022 441.437 155.022 451.51C155.022 520.123 210.682 571.466 274.978 571.466C287.931 571.466 300.407 569.549 312.883 565.228C334.473 586.341 364.222 599.776 396.852 599.776Z';

function addGptIcon(container) {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', '118 119 485 481');
  svg.setAttribute('aria-hidden', 'true');
  svg.setAttribute('focusable', 'false');
  const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path.setAttribute('d', GPT_ICON_PATH);
  path.setAttribute('fill', 'currentColor');
  svg.append(path);
  container.classList.add('gpt-icon');
  container.append(svg);
}

function isOpenAiModel(model) {
  return model?.provider === 'Codex';
}

async function request(path, options) {
  const response = await fetch(path, { ...options, headers: options?.body instanceof FormData ? options?.headers : { 'Content-Type': 'application/json', ...(options?.headers || {}) } });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || t('requestFailed', { status: response.status }));
  return data;
}

let toastTimer;
function toast(message) {
  $('toast').textContent = message;
  $('toast').classList.remove('hidden');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => $('toast').classList.add('hidden'), 4200);
}

function modelName(model) {
  if (model.model === 'gpt-6-luna') return 'GPT-6 Luna';
  if (model.model === 'gpt-5.6-luna') return 'GPT-5.6 Luna';
  return model.model;
}

function addResidentIcon(icon, model) {
  if (isOpenAiModel(model)) addGptIcon(icon);
  else icon.textContent = '✳';
}

function roomModel(room) {
  return room.members.length === 1
    ? state.residents.find((model) => model.key === room.members[0])
    : null;
}

function renderMarkdown(container, source) {
  if (!markdownRenderer) { container.textContent = source; return; }
  try {
    container.classList.add('markdown');
    container.innerHTML = markdownRenderer.render(source);
    for (const link of container.querySelectorAll('a')) {
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
    }
    for (const table of container.querySelectorAll('table')) {
      const scroller = document.createElement('div');
      scroller.className = 'markdown-table-scroll';
      table.replaceWith(scroller);
      scroller.append(table);
    }
  } catch {
    container.classList.remove('markdown');
    container.textContent = source;
  }
}

function renderSidebar() {
  const models = $('model-list');
  models.replaceChildren();
  $('model-count').textContent = String(state.residents.length);
  const activeRoom = state.rooms.find((room) => room.id === state.selected);
  for (const model of state.residents) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `room-item model-item${activeRoom?.members.length === 1 && activeRoom.members[0] === model.key && activeRoom.incognito === state.incognitoMode ? ' active' : ''}`;
    button.addEventListener('click', () => openSolo(model.key));
    const icon = document.createElement('span');
    icon.className = 'room-icon';
    addResidentIcon(icon, model);
    const copy = document.createElement('span');
    copy.className = 'room-copy';
    const title = document.createElement('strong');
    title.textContent = modelName(model);
    const sub = document.createElement('small');
    sub.textContent = `${model.provider} · ${model.key} · ${t(state.incognitoMode ? 'incognito' : 'saved')}`;
    copy.append(title, sub);
    const online = document.createElement('span');
    online.className = 'online-indicator';
    online.title = t('online');
    button.append(icon, copy, online);
    models.append(button);
  }
  if (!state.residents.length) {
    const empty = document.createElement('div');
    empty.className = 'sidebar-empty';
    empty.textContent = t('noResidents');
    models.append(empty);
  }

  const groups = $('group-list');
  groups.replaceChildren();
  const groupRooms = state.rooms.filter((room) => room.members.length > 1 && !room.archived);
  $('group-count').textContent = String(groupRooms.length);
  for (const room of [...groupRooms].reverse()) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `room-item${room.id === state.selected ? ' active' : ''}`;
    button.addEventListener('click', () => selectRoom(room.id));
    const icon = document.createElement('span');
    icon.className = 'room-icon';
    icon.textContent = '◎';
    const copy = document.createElement('span');
    copy.className = 'room-copy';
    const title = document.createElement('strong');
    title.textContent = room.name;
    const sub = document.createElement('small');
    sub.textContent = room.incognito ? t('incognitoGroup') : t('groupMeta', { models: room.members.length, messages: room.messages.length });
    copy.append(title, sub);
    button.append(icon, copy);
    if (room.busy) {
      const busy = document.createElement('span');
      busy.className = 'room-busy';
      busy.title = t('working');
      button.append(busy);
    }
    groups.append(button);
  }
  if (!groupRooms.length) {
    const empty = document.createElement('div');
    empty.className = 'sidebar-empty';
    empty.textContent = t('noGroups');
    groups.append(empty);
  }
}

function archiveFilterParts(kind) {
  return { trigger: $(`archive-${kind}-filter`), menu: $(`archive-${kind}-options`) };
}

function setArchiveFilterValue(kind, value) {
  const { trigger, menu } = archiveFilterParts(kind);
  const options = [...menu.querySelectorAll('[role="option"]')];
  const selected = options.find((option) => option.dataset.value === value) || options[0];
  if (!selected) return;
  trigger.dataset.value = selected.dataset.value;
  trigger.querySelector('.archive-filter-text').textContent = selected.textContent;
  trigger.setAttribute('aria-label', t(kind === 'type' ? 'chatTypeValue' : 'modelFilterValue', { value: selected.textContent }));
  for (const option of options) option.setAttribute('aria-selected', String(option === selected));
}

function closeArchiveFilters(refocus = false) {
  let openTrigger = null;
  for (const kind of ['type', 'member']) {
    const { trigger, menu } = archiveFilterParts(kind);
    if (trigger.getAttribute('aria-expanded') === 'true') openTrigger = trigger;
    trigger.setAttribute('aria-expanded', 'false');
    menu.classList.add('hidden');
  }
  if (refocus) openTrigger?.focus();
  return Boolean(openTrigger);
}

function openArchiveFilter(kind, focusLast = false) {
  closeArchiveFilters();
  const { trigger, menu } = archiveFilterParts(kind);
  trigger.setAttribute('aria-expanded', 'true');
  menu.classList.remove('hidden');
  const options = [...menu.querySelectorAll('[role="option"]')];
  (focusLast ? options.at(-1) : options.find((option) => option.getAttribute('aria-selected') === 'true') || options[0])?.focus();
}

function renderArchivedRooms() {
  const archive = $('archive-list');
  archive.replaceChildren();
  const archivedRooms = state.rooms.filter((room) => room.archived);
  $('archive-count').textContent = String(archivedRooms.length);
  const modelFilter = $('archive-member-filter');
  const previousModel = modelFilter.dataset.value;
  const memberKeys = [...new Set(archivedRooms.flatMap((room) => room.members))].sort();
  const modelMenu = $('archive-member-options');
  modelMenu.replaceChildren();
  for (const key of ['all', ...memberKeys]) {
    const option = document.createElement('button');
    option.type = 'button';
    option.className = 'archive-filter-option';
    option.setAttribute('role', 'option');
    option.tabIndex = -1;
    option.dataset.value = key;
    option.textContent = key === 'all' ? t('allModels') : modelName(state.residents.find((resident) => resident.key === key) || { model: key });
    modelMenu.append(option);
  }
  setArchiveFilterValue('member', memberKeys.includes(previousModel) ? previousModel : 'all');
  const query = $('archive-search').value.trim().toLocaleLowerCase();
  const type = $('archive-type-filter').dataset.value;
  const member = modelFilter.dataset.value;
  const visibleRooms = archivedRooms.filter((room) => {
    if (type === 'solo' && room.members.length !== 1) return false;
    if (type === 'group' && room.members.length < 2) return false;
    if (member !== 'all' && !room.members.includes(member)) return false;
    if (!query) return true;
    return room.name.toLocaleLowerCase().includes(query)
      || room.members.some((key) => key.toLocaleLowerCase().includes(query))
      || room.messages.some((message) => message.text.toLocaleLowerCase().includes(query));
  });
  $('archive-visible-count').textContent = t('chatCount', { count: visibleRooms.length });
  for (const room of [...visibleRooms].sort((a, b) => b.last_activity_ms - a.last_activity_ms)) {
    const row = document.createElement('div');
    row.className = 'archive-chat-row';
    row.setAttribute('role', 'listitem');
    const model = roomModel(room);
    const copy = document.createElement('div');
    copy.className = 'archive-chat-copy';
    const title = document.createElement('strong');
    const latestUserMessage = [...room.messages].reverse().find((message) => message.role === 'user' && message.text.trim());
    title.textContent = room.members.length === 1 ? latestUserMessage?.text || room.name : room.name;
    title.title = title.textContent;
    const details = document.createElement('small');
    details.className = 'archive-chat-meta';
    const date = room.messages.at(-1)?.created_at || room.last_activity_ms;
    const when = date ? new Date(date).toLocaleString(language === 'zh' ? 'zh-CN' : 'en-US', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : t('noMessages');
    const members = model ? modelName(model) : room.members.length === 1 ? room.members[0] : t('modelCount', { count: room.members.length });
    details.textContent = `${when} · ${members} · ${t('messageCount', { count: room.messages.length })}`;
    copy.append(title, details);
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'settings-unarchive';
    button.textContent = t('unarchive');
    button.setAttribute('aria-label', t('unarchiveRoom', { name: room.name }));
    button.addEventListener('click', () => unarchiveRoom(room.id, button));
    row.append(copy, button);
    archive.append(row);
  }
  if (!visibleRooms.length) {
    const empty = document.createElement('div');
    empty.className = 'archive-chat-empty';
    empty.textContent = t(archivedRooms.length ? 'noMatchingArchived' : 'noArchived');
    archive.append(empty);
  }
}

function renderMessages(room) {
  const fingerprint = JSON.stringify(room.messages);
  if (fingerprint === state.fingerprint) return;
  const container = $('messages');
  const scroller = $('conversation');
  const nearBottom = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 120;
  container.replaceChildren();
  for (const message of room.messages) {
    const row = document.createElement('article');
    row.className = `message ${message.role}`;
    const icon = document.createElement('div');
    icon.className = 'message-icon';
    if ((message.role === 'agent' || message.role === 'summary') && isOpenAiModel(state.residents.find((model) => model.key === message.author))) {
      addGptIcon(icon);
    } else {
      icon.textContent = message.role === 'user' ? t('you') : message.role === 'error' ? '!' : message.role === 'summary' ? '✓' : '✳';
    }
    const body = document.createElement('div');
    body.className = 'message-body';
    const meta = document.createElement('div');
    meta.className = 'message-meta';
    const name = document.createElement('span');
    name.textContent = message.role === 'user' ? t('you') : message.author;
    meta.append(name);
    if (message.role === 'summary') {
      const tag = document.createElement('span');
      tag.className = 'summary-tag';
      tag.textContent = t('finalAnswer');
      meta.append(tag);
    }
    const text = document.createElement('div');
    text.className = 'message-text';
    if (message.role === 'agent' || message.role === 'summary') renderMarkdown(text, message.text);
    else text.textContent = message.text;
    body.append(meta);
    if (message.text) body.append(text);
    if (message.attachments?.length) {
      const gallery = document.createElement('div');
      gallery.className = 'message-attachments';
      for (const attachment of message.attachments) {
        const link = document.createElement('a');
        link.href = attachment.url;
        link.download = attachment.name;
        link.target = '_blank';
        link.rel = 'noopener';
        const preview = document.createElement('img');
        preview.src = attachment.url;
        preview.alt = attachment.name;
        preview.loading = 'lazy';
        link.append(preview);
        const label = document.createElement('span');
        label.textContent = attachment.name;
        link.append(label);
        gallery.append(link);
      }
      body.append(gallery);
    }
    row.append(icon, body);
    container.append(row);
  }
  state.fingerprint = fingerprint;
  if (nearBottom || room.messages.length <= 2) requestAnimationFrame(() => { scroller.scrollTop = scroller.scrollHeight; });
}

function updateWorking() {
  const room = state.room;
  if (!room?.busy || state.selected !== room.id) return;
  const lastMessage = room.messages.at(-1);
  const elapsed = lastMessage ? Math.max(0, Math.floor((Date.now() - lastMessage.created_at) / 1000)) : 0;
  const member = room.active_member || 'Resident';
  const firstReply = room.messages.length === 1 && room.messages[0].role === 'user';
  const status = t(firstReply && elapsed >= 30 ? 'waitingFirstReply' : 'working');
  $('working-label').textContent = t('workingElapsed', { member, status, seconds: elapsed });
}

function renderRoom(room) {
  state.room = room;
  const soloModel = roomModel(room);
  const title = soloModel ? `${modelName(soloModel)}${room.incognito ? ' · ' + t('incognito') : ''}` : room.name;
  $('welcome').classList.add('hidden');
  $('conversation').classList.remove('hidden');
  $('composer-wrap').classList.remove('hidden');
  $('room-title').textContent = title;
  $('mode-pill').textContent = room.incognito ? t('incognitoConversation') : room.members.length === 1 ? t('soloTask') : t('collaboration', { count: room.members.length });
  $('archive-room').classList.toggle('hidden', room.incognito || room.archived);
  $('composer-note').textContent = room.incognito ? t('incognitoNote')
    : room.members.length === 1
    ? t('soloNote')
    : t('groupNote', { member: room.members[0] });
  renderMessages(room);
  $('working').classList.toggle('hidden', !room.busy);
  updateWorking();
  $('message-input').disabled = room.busy || room.archived;
  $('send-button').disabled = room.busy || room.archived;
  $('file-input').disabled = room.busy || room.archived;
  $('message-input').placeholder = t(room.archived ? 'unarchiveToContinue' : room.busy ? 'waitForResident' : 'messagePlaceholder');
}

async function refreshRooms() {
  state.rooms = await request('/api/rooms');
  renderSidebar();
  renderArchivedRooms();
}

async function refreshRoom(id = state.selected) {
  if (id == null) return;
  const room = await request(`/api/rooms/${id}`);
  if (state.selected !== id) return;
  if (room.archived) {
    clearSelectedRoom();
    await refreshRooms();
    return;
  }
  renderRoom(room);
  await refreshRooms();
}

function clearSelectedRoom() {
  if (state.events) { state.events.close(); state.events = null; }
  clearInterval(heartbeatTimer);
  if (state.selected) {
    request(`/api/rooms/${state.selected}/leave`, { method: 'POST', body: JSON.stringify({ client: clientId }) }).catch(() => {});
  }
  state.selected = null;
  state.room = null;
  state.files = [];
  state.fingerprint = '';
  sessionStorage.removeItem('norma-room-id');
  renderPendingFiles();
  $('message-input').value = '';
  $('message-input').style.height = '';
  $('welcome').classList.remove('hidden');
  $('conversation').classList.add('hidden');
  $('composer-wrap').classList.add('hidden');
  $('archive-room').classList.add('hidden');
  $('room-title').textContent = t('startConversation');
  $('mode-pill').textContent = t('chooseOnlineModel');
  renderSidebar();
}

async function selectRoom(id) {
  if (state.events) { state.events.close(); state.events = null; }
  clearInterval(heartbeatTimer);
  if (state.selected && state.selected !== id) {
    request(`/api/rooms/${state.selected}/leave`, { method: 'POST', body: JSON.stringify({ client: clientId }) }).catch(() => {});
  }
  if (state.selected !== id) { state.files = []; renderPendingFiles(); }
  state.selected = id;
  state.room = null;
  $('message-input').disabled = true;
  $('send-button').disabled = true;
  state.fingerprint = '';
  sessionStorage.setItem('norma-room-id', String(id));
  $('sidebar').classList.remove('open');
  renderSidebar();
  try { await refreshRoom(id); } catch (error) { toast(error.message); return; }
  if (state.selected !== id) return;
  const beat = () => request(`/api/rooms/${id}/heartbeat`, { method: 'POST', body: JSON.stringify({ client: clientId }) }).catch(() => {});
  beat();
  heartbeatTimer = setInterval(beat, 15000);
  const source = new EventSource(`/api/rooms/${id}/events`);
  source.addEventListener('update', () => refreshRoom(id).catch((error) => toast(error.message)));
  state.events = source;
}

$('archive-room').addEventListener('click', async () => {
  if (!state.room || state.room.archived) return;
  const id = state.room.id;
  try {
    await request(`/api/rooms/${id}/archive`, {
      method: 'POST', body: JSON.stringify({ archived: true }),
    });
    if (state.selected === id) clearSelectedRoom();
    await refreshRooms();
    toast(t('archivedToast'));
  } catch (error) { toast(error.message); }
});

async function unarchiveRoom(id, button) {
  button.disabled = true;
  try {
    await request(`/api/rooms/${id}/archive`, {
      method: 'POST', body: JSON.stringify({ archived: false }),
    });
    await refreshRooms();
    closeSettings();
    await selectRoom(id);
    if (state.selected === id && state.room) $('message-input').focus();
    toast(t('unarchivedToast'));
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; }
}

window.addEventListener('pagehide', () => {
  if (!state.selected) return;
  navigator.sendBeacon(`/api/rooms/${state.selected}/leave`, new Blob([JSON.stringify({ client: clientId })], { type: 'application/json' }));
});

async function openSolo(key) {
  try {
    const route = state.incognitoMode ? 'incognito' : 'solo';
    const room = await request(`/api/rooms/${route}/${encodeURIComponent(key)}`, { method: 'POST' });
    await refreshRooms();
    await selectRoom(room.id);
    $('message-input').focus();
  } catch (error) { toast(error.message); }
}

async function refreshMemory() {
  const status = await request('/api/memory');
  state.memoryStatus = status;
  $('memory-enabled').checked = status.enabled;
  $('memory-enabled').title = t('memoryStats', status);
}

$('memory-enabled').addEventListener('change', async (event) => {
  event.target.disabled = true;
  try {
    const status = await request('/api/memory', { method: 'POST', body: JSON.stringify({ enabled: event.target.checked }) });
    event.target.checked = status.enabled;
    toast(t(status.enabled ? 'memoryEnabled' : 'memoryDisabled'));
    await refreshMemory();
  } catch (error) { event.target.checked = !event.target.checked; toast(error.message); }
  finally { event.target.disabled = false; }
});

async function loadResidents() {
  state.residents = await request('/api/residents');
  state.residentsLoaded = true;
  $('resident-count').textContent = t('residentCount', { count: state.residents.length });
  renderSidebar();
  renderResidentOptions();
}

function selectedMembers() {
  return [...$('resident-options').querySelectorAll('input:checked')].map((input) => input.value);
}

function updateModePreview() {
  const count = selectedMembers().length;
  $('selected-count').textContent = t('selectedCount', { count });
  $('mode-preview').textContent = count < 2 ? t('chooseTwoDifferent') : `${t('agentsCollaborating', { count })}${state.incognitoMode ? ' · ' + t('incognito') : ''}`;
  $('create-button').disabled = count < 2;
}

function renderResidentOptions() {
  const options = $('resident-options');
  const prior = new Set(selectedMembers());
  options.replaceChildren();
  if (!state.residents.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-residents';
    empty.textContent = t('noLlmResidents');
    options.append(empty);
  } else if (state.residents.length === 1) {
    const hint = document.createElement('div');
    hint.className = 'empty-residents';
    hint.textContent = t('needAnotherResident');
    options.append(hint);
  }
  for (const resident of state.residents) {
    const label = document.createElement('label');
    label.className = 'resident-option';
    const check = document.createElement('input');
    check.type = 'checkbox';
    check.value = resident.key;
    check.checked = prior.has(resident.key);
    check.addEventListener('change', updateModePreview);
    const icon = document.createElement('span');
    icon.className = 'resident-option-icon';
    addResidentIcon(icon, resident);
    const copy = document.createElement('span');
    const title = document.createElement('strong');
    title.textContent = modelName(resident);
    const hint = document.createElement('small');
    hint.textContent = `${resident.provider} · ${resident.key}`;
    copy.append(title, hint);
    label.append(check, icon, copy);
    options.append(label);
  }
  updateModePreview();
}

function openModal() {
  $('modal-backdrop').classList.remove('hidden');
  $('new-room-name').focus();
  loadResidents().catch((error) => toast(error.message));
}
function closeModal() { $('modal-backdrop').classList.add('hidden'); }
function openSettings() {
  $('archive-search').value = '';
  setArchiveFilterValue('type', 'all');
  setArchiveFilterValue('member', 'all');
  $('settings-page').classList.remove('hidden');
  showSettingsView('archives');
  refreshRooms().catch((error) => toast(error.message));
  refreshMemory().catch((error) => toast(error.message));
  $('close-settings').focus();
}
function closeSettings() {
  closeArchiveFilters();
  $('settings-page').classList.add('hidden');
  $('settings-button').focus();
}

function showSettingsView(view) {
  closeArchiveFilters();
  const archives = view === 'archives';
  $('settings-archives-view').classList.toggle('hidden', !archives);
  $('settings-preferences-view').classList.toggle('hidden', archives);
  $('settings-page').querySelector('.settings-main').scrollTop = 0;
  for (const [button, active] of [[$('settings-nav-archives'), archives], [$('settings-nav-preferences'), !archives]]) {
    button.classList.toggle('active', active);
    if (active) button.setAttribute('aria-current', 'page');
    else button.removeAttribute('aria-current');
  }
  if (archives) renderArchivedRooms();
  else refreshMemory().catch((error) => toast(error.message));
}

$('new-room').addEventListener('click', openModal);
$('incognito-mode').checked = state.incognitoMode;
$('incognito-mode').addEventListener('change', (event) => {
  state.incognitoMode = event.target.checked;
  try { localStorage.setItem('norma-incognito-mode', String(state.incognitoMode)); } catch { /* Storage may be unavailable. */ }
  renderSidebar();
  updateModePreview();
  toast(t(state.incognitoMode ? 'incognitoEnabled' : 'incognitoDisabled'));
});
$('settings-button').addEventListener('click', openSettings);
$('close-settings').addEventListener('click', closeSettings);
$('settings-page').addEventListener('click', (event) => { if (event.target === $('settings-page')) closeSettings(); });
$('settings-nav-archives').addEventListener('click', () => showSettingsView('archives'));
$('settings-nav-preferences').addEventListener('click', () => showSettingsView('preferences'));
$('archive-search').addEventListener('input', renderArchivedRooms);
for (const kind of ['type', 'member']) {
  const { trigger, menu } = archiveFilterParts(kind);
  trigger.addEventListener('click', () => {
    if (trigger.getAttribute('aria-expanded') === 'true') closeArchiveFilters();
    else openArchiveFilter(kind);
  });
  trigger.addEventListener('keydown', (event) => {
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
      event.preventDefault();
      openArchiveFilter(kind, event.key === 'ArrowUp' || event.key === 'End');
    }
  });
  menu.addEventListener('click', (event) => {
    const option = event.target.closest('[role="option"]');
    if (!option || !menu.contains(option)) return;
    setArchiveFilterValue(kind, option.dataset.value);
    closeArchiveFilters(true);
    renderArchivedRooms();
  });
  menu.addEventListener('keydown', (event) => {
    const options = [...menu.querySelectorAll('[role="option"]')];
    const current = options.indexOf(document.activeElement);
    let next = current;
    if (event.key === 'ArrowDown') next = (current + 1) % options.length;
    else if (event.key === 'ArrowUp') next = (current - 1 + options.length) % options.length;
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = options.length - 1;
    else if (event.key === 'Tab') { closeArchiveFilters(); return; }
    else return;
    event.preventDefault();
    options[next]?.focus();
  });
}
document.addEventListener('pointerdown', (event) => {
  if (!event.target.closest('.archive-filter')) closeArchiveFilters();
});
$('welcome-create').addEventListener('click', () => {
  if (state.residents[0]) openSolo(state.residents[0].key);
  else toast(t('noOnlineResident'));
});
$('close-modal').addEventListener('click', closeModal);
$('modal-backdrop').addEventListener('click', (event) => { if (event.target === $('modal-backdrop')) closeModal(); });
document.addEventListener('keydown', (event) => { if (event.key === 'Escape') { if (closeArchiveFilters(true)) { event.preventDefault(); return; } closeModal(); if (!$('settings-page').classList.contains('hidden')) closeSettings(); } });
$('menu-toggle').addEventListener('click', () => $('sidebar').classList.toggle('open'));

$('create-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const members = selectedMembers();
  if (members.length < 2) return;
  $('create-button').disabled = true;
  try {
    const room = await request('/api/rooms', { method: 'POST', body: JSON.stringify({ name: $('new-room-name').value, members, incognito: state.incognitoMode }) });
    closeModal();
    $('create-form').reset();
    await refreshRooms();
    await selectRoom(room.id);
    $('message-input').focus();
  } catch (error) { toast(error.message); }
  finally { updateModePreview(); }
});

$('composer').addEventListener('submit', async (event) => {
  event.preventDefault();
  const text = $('message-input').value.trim();
  if ((!text && !state.files.length) || !state.selected || !state.room || state.room.busy) return;
  $('send-button').disabled = true;
  try {
    let room;
    if (state.files.length) {
      const form = new FormData();
      form.append('text', text);
      for (const file of state.files) form.append('files', file, file.name);
      room = await request(`/api/rooms/${state.selected}/uploads`, { method: 'POST', body: form });
    } else {
      room = await request(`/api/rooms/${state.selected}/messages`, { method: 'POST', body: JSON.stringify({ text }) });
    }
    $('message-input').value = '';
    state.files = [];
    renderPendingFiles();
    $('message-input').style.height = '';
    renderRoom(room);
    await refreshRooms();
  } catch (error) { toast(error.message); $('send-button').disabled = false; }
});

function addFiles(files) {
  for (const file of files) {
    if (!['image/png', 'image/jpeg', 'image/webp', 'image/gif'].includes(file.type)) { toast(t('supportedImages')); continue; }
    if (file.size > 8 * 1024 * 1024) { toast(t('imageSizeLimit')); continue; }
    if (state.files.length >= 4) { toast(t('imageCountLimit')); break; }
    state.files.push(file);
  }
  renderPendingFiles();
}

function renderPendingFiles() {
  const list = $('pending-files');
  list.replaceChildren();
  for (const [index, file] of state.files.entries()) {
    const chip = document.createElement('span');
    chip.className = 'pending-file';
    chip.textContent = file.name;
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.textContent = '×';
    remove.setAttribute('aria-label', t('removeFile', { name: file.name }));
    remove.addEventListener('click', () => { state.files.splice(index, 1); renderPendingFiles(); });
    chip.append(remove);
    list.append(chip);
  }
}

$('file-input').addEventListener('change', (event) => {
  addFiles(event.target.files);
  event.target.value = '';
});
$('message-input').addEventListener('paste', (event) => {
  const files = [...event.clipboardData.files].filter((file) => file.type.startsWith('image/'));
  if (files.length) { event.preventDefault(); addFiles(files); }
});
$('composer').addEventListener('dragover', (event) => { event.preventDefault(); });
$('composer').addEventListener('drop', (event) => {
  event.preventDefault();
  addFiles(event.dataTransfer.files);
});

$('message-input').addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    $('composer').requestSubmit();
  }
});
$('message-input').addEventListener('input', (event) => {
  event.target.style.height = 'auto';
  event.target.style.height = `${Math.min(event.target.scrollHeight, 150)}px`;
});

function applyLanguage(nextLanguage) {
  language = nextLanguage === 'zh' ? 'zh' : 'en';
  try { localStorage.setItem('norma-ui-language', language); } catch { /* Storage may be unavailable. */ }
  document.documentElement.lang = language === 'zh' ? 'zh-CN' : 'en';
  document.title = t('pageTitle');
  for (const node of document.querySelectorAll('[data-i18n]')) node.textContent = t(node.dataset.i18n);
  for (const [attribute, key] of [['title', 'i18nTitle'], ['placeholder', 'i18nPlaceholder'], ['aria-label', 'i18nAriaLabel']]) {
    for (const node of document.querySelectorAll(`[data-${key.replace(/[A-Z]/g, (letter) => '-' + letter.toLowerCase())}]`)) {
      node.setAttribute(attribute, t(node.dataset[key]));
    }
  }
  for (const code of ['en', 'zh']) $('language-' + code).setAttribute('aria-pressed', String(language === code));
  $('resident-count').textContent = state.residentsLoaded ? t('residentCount', { count: state.residents.length }) : t('connecting');
  renderSidebar();
  renderArchivedRooms();
  setArchiveFilterValue('type', $('archive-type-filter').dataset.value);
  if (state.residentsLoaded) renderResidentOptions();
  else updateModePreview();
  if (state.memoryStatus) $('memory-enabled').title = t('memoryStats', state.memoryStatus);
  state.fingerprint = '';
  if (state.room) renderRoom(state.room);
  else {
    $('room-title').textContent = t('startConversation');
    $('mode-pill').textContent = t('chooseOnlineModel');
  }
  renderPendingFiles();
}

$('language-en').addEventListener('click', () => applyLanguage('en'));
$('language-zh').addEventListener('click', () => applyLanguage('zh'));
applyLanguage(language);

(async () => {
  try {
    await Promise.all([loadResidents(), refreshRooms(), refreshMemory()]);
    const saved = Number(sessionStorage.getItem('norma-room-id'));
    const savedRoom = state.rooms.find((room) => room.id === saved);
    if (savedRoom && !savedRoom.archived) await selectRoom(savedRoom.id);
    else if (savedRoom?.archived) sessionStorage.removeItem('norma-room-id');
  } catch (error) { toast(error.message); }
})();

setInterval(updateWorking, 1000);
