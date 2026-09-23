const $ = (id) => document.getElementById(id);
const state = { residents: [], rooms: [], selected: null, room: null, events: null, fingerprint: '', files: [] };
const clientId = crypto.randomUUID();
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
  if (!response.ok) throw new Error(data.error || `请求失败 (${response.status})`);
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
  return model.model === 'gpt-6-luna' ? 'GPT-6 Luna' : model.model;
}

function renderSidebar() {
  const models = $('model-list');
  models.replaceChildren();
  $('model-count').textContent = String(state.residents.length);
  const activeRoom = state.rooms.find((room) => room.id === state.selected);
  for (const model of state.residents) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `room-item model-item${activeRoom?.members.length === 1 && activeRoom.members[0] === model.key ? ' active' : ''}`;
    button.addEventListener('click', () => openSolo(model.key));
    const icon = document.createElement('span');
    icon.className = 'room-icon';
    if (isOpenAiModel(model)) addGptIcon(icon);
    else icon.textContent = '✳';
    const copy = document.createElement('span');
    copy.className = 'room-copy';
    const title = document.createElement('strong');
    title.textContent = modelName(model);
    const sub = document.createElement('small');
    sub.textContent = `${model.provider} · ${model.key}`;
    copy.append(title, sub);
    const online = document.createElement('span');
    online.className = 'online-indicator';
    online.title = '在线';
    button.append(icon, copy, online);
    models.append(button);
  }
  if (!state.residents.length) {
    const empty = document.createElement('div');
    empty.className = 'sidebar-empty';
    empty.textContent = '暂无在线 LLM Resident';
    models.append(empty);
  }

  const groups = $('group-list');
  groups.replaceChildren();
  const groupRooms = state.rooms.filter((room) => !room.archived);
  $('group-count').textContent = String(groupRooms.length);
  for (const room of [...groupRooms].reverse()) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `room-item${room.id === state.selected ? ' active' : ''}`;
    button.addEventListener('click', () => selectRoom(room.id));
    const icon = document.createElement('span');
    icon.className = 'room-icon';
    icon.textContent = room.incognito ? '◌' : room.members.length > 1 ? '◎' : '●';
    const copy = document.createElement('span');
    copy.className = 'room-copy';
    const title = document.createElement('strong');
    title.textContent = room.name;
    const sub = document.createElement('small');
    sub.textContent = room.incognito ? '无痕 · 仅当前运行期间' : `${room.members.length} 个模型 · ${room.messages.length} 条消息`;
    copy.append(title, sub);
    button.append(icon, copy);
    if (room.busy) {
      const busy = document.createElement('span');
      busy.className = 'room-busy';
      busy.title = '正在处理';
      button.append(busy);
    }
    groups.append(button);
  }
  if (!groupRooms.length) {
    const empty = document.createElement('div');
    empty.className = 'sidebar-empty';
    empty.textContent = '还没有会话';
    groups.append(empty);
  }

  const archive = $('archive-list');
  archive.replaceChildren();
  const archivedRooms = state.rooms.filter((room) => room.archived);
  $('archive-count').textContent = String(archivedRooms.length);
  for (const room of [...archivedRooms].reverse()) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `room-item${room.id === state.selected ? ' active' : ''}`;
    button.addEventListener('click', () => selectRoom(room.id));
    const icon = document.createElement('span');
    icon.className = 'room-icon';
    icon.textContent = '▣';
    const copy = document.createElement('span');
    copy.className = 'room-copy';
    const title = document.createElement('strong');
    title.textContent = room.name;
    const sub = document.createElement('small');
    sub.textContent = `${room.messages.length} 条消息 · #${room.id}`;
    copy.append(title, sub);
    button.append(icon, copy);
    archive.append(button);
  }
  if (!archivedRooms.length) {
    const empty = document.createElement('div');
    empty.className = 'sidebar-empty';
    empty.textContent = '暂无归档会话';
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
    if (message.role === 'agent' && isOpenAiModel(state.residents.find((model) => model.key === message.author))) {
      addGptIcon(icon);
    } else {
      icon.textContent = message.role === 'user' ? '你' : message.role === 'error' ? '!' : message.role === 'summary' ? '✓' : '✳';
    }
    const body = document.createElement('div');
    body.className = 'message-body';
    const meta = document.createElement('div');
    meta.className = 'message-meta';
    const name = document.createElement('span');
    name.textContent = message.author;
    meta.append(name);
    if (message.role === 'summary') {
      const tag = document.createElement('span');
      tag.className = 'summary-tag';
      tag.textContent = '协作总结';
      meta.append(tag);
    }
    const text = document.createElement('div');
    text.className = 'message-text';
    text.textContent = message.text;
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
  const status = firstReply && elapsed >= 30 ? '首次回复等待中' : '正在处理';
  $('working-label').textContent = `${member} ${status} · 已等待 ${elapsed} 秒`;
}

function renderRoom(room) {
  state.room = room;
  const soloModel = room.members.length === 1 ? state.residents.find((model) => model.key === room.members[0]) : null;
  const title = soloModel ? modelName(soloModel) : room.name;
  $('welcome').classList.add('hidden');
  $('conversation').classList.remove('hidden');
  $('composer-wrap').classList.remove('hidden');
  $('room-title').textContent = title;
  $('mode-pill').textContent = room.incognito ? '无痕对话' : room.members.length === 1 ? '单 Agent 任务' : `多 Agent 协作 · ${room.members.length}`;
  $('archive-room').classList.toggle('hidden', room.incognito);
  $('archive-room').textContent = room.archived ? '取消归档' : '归档';
  $('composer-note').textContent = room.incognito ? '无痕会话不会保存到本地历史，也不会提取长期记忆。'
    : room.members.length === 1
    ? '同一个 Resident 会记得它参与的私聊和群聊。重要结论请核实。'
    : `可用 @{${room.members[0]}} 定向提问；不 @ 时全体协作。`;
  renderMessages(room);
  $('working').classList.toggle('hidden', !room.busy);
  updateWorking();
  $('message-input').disabled = room.busy || room.archived;
  $('send-button').disabled = room.busy || room.archived;
  $('file-input').disabled = room.busy || room.archived;
  $('message-input').placeholder = room.archived ? '取消归档后可继续对话' : room.busy ? '等待 Resident 完成当前任务…' : '发送消息给 Resident…';
}

async function refreshRooms() {
  state.rooms = await request('/api/rooms');
  renderSidebar();
}

async function refreshRoom(id = state.selected) {
  if (id == null) return;
  const room = await request(`/api/rooms/${id}`);
  if (state.selected !== id) return;
  renderRoom(room);
  await refreshRooms();
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
  if (!state.room) return;
  try {
    const room = await request(`/api/rooms/${state.room.id}/archive`, {
      method: 'POST', body: JSON.stringify({ archived: !state.room.archived }),
    });
    renderRoom(room);
    await refreshRooms();
  } catch (error) { toast(error.message); }
});

window.addEventListener('pagehide', () => {
  if (!state.selected) return;
  navigator.sendBeacon(`/api/rooms/${state.selected}/leave`, new Blob([JSON.stringify({ client: clientId })], { type: 'application/json' }));
});

async function openSolo(key) {
  try {
    const room = await request(`/api/rooms/solo/${encodeURIComponent(key)}`, { method: 'POST' });
    await refreshRooms();
    await selectRoom(room.id);
    $('message-input').focus();
  } catch (error) { toast(error.message); }
}

async function openIncognito(key) {
  try {
    const room = await request(`/api/rooms/incognito/${encodeURIComponent(key)}`, { method: 'POST' });
    await refreshRooms();
    await selectRoom(room.id);
    $('message-input').focus();
  } catch (error) { toast(error.message); }
}

async function refreshMemory() {
  const status = await request('/api/memory');
  $('memory-enabled').checked = status.enabled;
  $('memory-enabled').title = `原始记忆 ${status.stored} 条 · cache ${status.cached} 条 · hot ${status.hot} 条`;
}

$('memory-enabled').addEventListener('change', async (event) => {
  event.target.disabled = true;
  try {
    const status = await request('/api/memory', { method: 'POST', body: JSON.stringify({ enabled: event.target.checked }) });
    event.target.checked = status.enabled;
    toast(status.enabled ? '长期记忆已开启' : '长期记忆已关闭');
    await refreshMemory();
  } catch (error) { event.target.checked = !event.target.checked; toast(error.message); }
  finally { event.target.disabled = false; }
});

async function loadResidents() {
  state.residents = await request('/api/residents');
  $('resident-count').textContent = `${state.residents.length} 个 LLM Resident 在线`;
  renderSidebar();
  renderResidentOptions();
}

function selectedMembers() {
  return [...$('resident-options').querySelectorAll('input:checked')].map((input) => input.value);
}

function updateModePreview() {
  const count = selectedMembers().length;
  $('selected-count').textContent = `已选 ${count} 个`;
  $('mode-preview').textContent = count < 2 ? '至少选择 2 个不同模型' : `${count} 个 Agent 协作`;
  $('create-button').disabled = count < 2;
}

function renderResidentOptions() {
  const options = $('resident-options');
  const prior = new Set(selectedMembers());
  options.replaceChildren();
  if (!state.residents.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-residents';
    empty.textContent = '当前没有标注为 llm 的 Resident。请检查 Codex 登录和服务端启动日志。';
    options.append(empty);
  } else if (state.residents.length === 1) {
    const hint = document.createElement('div');
    hint.className = 'empty-residents';
    hint.textContent = '目前只有一个在线模型。还需接入另一个不同的 LLM Resident 才能创建群聊。';
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
    icon.textContent = '✳';
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

$('new-room').addEventListener('click', openModal);
$('new-incognito').addEventListener('click', () => {
  if (state.residents[0]) openIncognito(state.residents[0].key);
  else toast('当前没有在线的 LLM Resident');
});
$('welcome-create').addEventListener('click', () => {
  if (state.residents[0]) openSolo(state.residents[0].key);
  else toast('当前没有在线的 LLM Resident');
});
$('close-modal').addEventListener('click', closeModal);
$('modal-backdrop').addEventListener('click', (event) => { if (event.target === $('modal-backdrop')) closeModal(); });
document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeModal(); });
$('menu-toggle').addEventListener('click', () => $('sidebar').classList.toggle('open'));

$('create-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const members = selectedMembers();
  if (members.length < 2) return;
  $('create-button').disabled = true;
  try {
    const room = await request('/api/rooms', { method: 'POST', body: JSON.stringify({ name: $('new-room-name').value, members, incognito: $('group-incognito').checked }) });
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
    if (!['image/png', 'image/jpeg', 'image/webp', 'image/gif'].includes(file.type)) { toast('支持 PNG、JPEG、WebP 和 GIF'); continue; }
    if (file.size > 8 * 1024 * 1024) { toast('单张图片最多 8 MiB'); continue; }
    if (state.files.length >= 4) { toast('每条消息最多 4 张图片'); break; }
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
    remove.setAttribute('aria-label', `移除 ${file.name}`);
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

(async () => {
  try {
    await Promise.all([loadResidents(), refreshRooms(), refreshMemory()]);
    const saved = Number(sessionStorage.getItem('norma-room-id'));
    const savedRoom = state.rooms.find((room) => room.id === saved);
    if (savedRoom) await selectRoom(savedRoom.id);
  } catch (error) { toast(error.message); }
})();

setInterval(updateWorking, 1000);
