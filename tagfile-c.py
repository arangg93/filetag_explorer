# -*- coding: utf-8 -*-
"""
[상우고] Arang's 파일 태그 검색기 (v.0.1.4, 최적화판)
- 코드 중복 제거 및 최적화
- 메모리 사용량 감소
"""

import os
import sys
import sqlite3
import threading
import hashlib
import ctypes
import webbrowser
from ctypes import wintypes
from pathlib import Path
from datetime import datetime

from PySide6.QtCore import Qt, Signal, QTimer, QRectF
from PySide6.QtGui import QColor, QBrush, QShortcut, QKeySequence, QPen
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton,
    QFileDialog, QTableWidget, QTableWidgetItem, QListWidget, QListWidgetItem,
    QSplitter, QMessageBox, QLabel, QCheckBox, QAbstractItemView, QComboBox,
    QHeaderView, QProgressBar, QInputDialog, QMenu, QSizePolicy,
    QStyledItemDelegate, QStyleOptionViewItem, QStyle
)

# ========================= 전역 상수 =========================
DB_PATH = "filetags.db"
APP_TITLE = "[상우고] Arang's 파일 태그 검색기(v.0.1.4) / 제작 : 독서하는 경호t"
BTN_H, EDIT_H, SEARCH_W, SEARCH_BTN_W = 26, 28, 190, 60

PALETTE = [
    QColor(255,204,204), QColor(255,229,204), QColor(255,255,204), QColor(229,255,204),
    QColor(204,255,204), QColor(204,255,229), QColor(204,255,255), QColor(204,229,255),
    QColor(204,204,255), QColor(229,204,255), QColor(255,204,255), QColor(255,204,229),
    QColor(255,240,200), QColor(220,245,255), QColor(220,255,240), QColor(245,220,255)
]

# ========================= 유틸리티 =========================
def normalize_path(p: str) -> str:
    """경로 정규화"""
    if not p: return p
    np = os.path.normpath(p)
    return np + os.sep if len(np) == 2 and np[1] == ':' else np

def format_size_explorer(n: int) -> str:
    """Windows 탐색기 유사 크기 표기"""
    if n < 1024: return f"{n}B"
    kb = n / 1024.0
    if n < 1024**2: return f"{int(round(kb)):,}KB"
    mb = kb / 1024.0
    if n < 1024**3:
        return f"{mb:.1f}".rstrip("0").rstrip(".") + "MB"
    gb = mb / 1024.0
    return f"{gb:.1f}".rstrip("0").rstrip(".") + "GB"

def color_for_item(identifier, name: str = None) -> QColor:
    """태그/경로 색상 생성"""
    if isinstance(identifier, int):
        return PALETTE[identifier % len(PALETTE)]
    h = int(hashlib.sha1((name or identifier or '').encode()).hexdigest()[:2], 16)
    return PALETTE[h % len(PALETTE)]

# ========================= DB 관리 =========================
class DBManager:
    """DB 연결 및 쿼리 관리 클래스"""
    
    @staticmethod
    def get_conn():
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    
    @classmethod
    def init_db(cls):
        """DB 초기화"""
        conn = cls.get_conn()
        cur = conn.cursor()
        
        # 테이블 생성
        tables = [
            """CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY, path TEXT UNIQUE,
                size INTEGER, mtime REAL, hash TEXT);""",
            """CREATE TABLE IF NOT EXISTS tags(
                id INTEGER PRIMARY KEY, name TEXT UNIQUE, ord INTEGER);""",
            """CREATE TABLE IF NOT EXISTS file_tags(
                file_id INTEGER, tag_id INTEGER, UNIQUE(file_id, tag_id),
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE);""",
            """CREATE TABLE IF NOT EXISTS roots(
                path TEXT PRIMARY KEY, last_scanned REAL);""",
            """CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY, value TEXT);"""
        ]
        for sql in tables: cur.execute(sql)
        
        # 인덱스 생성
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);",
            "CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);",
            "CREATE INDEX IF NOT EXISTS idx_file_tags_file ON file_tags(file_id);",
            "CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime);"
        ]
        for idx in indices: cur.execute(idx)
        conn.commit()
        
        # tags.ord 백필
        cur.execute("SELECT COUNT(*) FROM tags WHERE ord IS NULL;")
        if cur.fetchone()[0]:
            cur.execute("SELECT id FROM tags WHERE ord IS NULL ORDER BY name;")
            for i, (tid,) in enumerate(cur.fetchall(), 1):
                cur.execute("UPDATE tags SET ord=? WHERE id=?;", (i, tid))
            conn.commit()
        conn.close()
    
    @classmethod
    def get_setting(cls, key, default=None):
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=?;", (key,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else default
    
    @classmethod
    def set_setting(cls, key, value):
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute("""INSERT INTO settings(key,value) VALUES(?,?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value;""", (key, value))
        conn.commit()
        conn.close()
    
    @classmethod
    def upsert_file(cls, path: str):
        """파일 메타 저장"""
        p = Path(path)
        if not p.is_file(): return
        st = p.stat()
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute("""INSERT INTO files(path,size,mtime,hash) VALUES(?,?,?,NULL)
                       ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime;""",
                    (normalize_path(str(p)), st.st_size, st.st_mtime))
        conn.commit()
        conn.close()
    
    @classmethod
    def count_and_stats(cls, root: str = None, is_disk: bool = False):
        """파일 개수 및 최대 mtime 조회 (DB 또는 디스크)"""
        if is_disk:
            if not root: return 0, 0.0
            total, max_m = 0, 0.0
            for dp, _, files in os.walk(normalize_path(root)):
                total += len(files)
                for fn in files:
                    try:
                        mt = os.path.getmtime(os.path.join(dp, fn))
                        if mt > max_m: max_m = mt
                    except: pass
            return total, max_m
        else:
            conn = cls.get_conn()
            cur = conn.cursor()
            if root:
                root = normalize_path(root)
                cur.execute("SELECT COUNT(*), COALESCE(MAX(mtime),0) FROM files WHERE path LIKE ?;",
                           (f"{root}%",))
            else:
                cur.execute("SELECT COUNT(*), COALESCE(MAX(mtime),0) FROM files;")
            n, mx = cur.fetchone()
            conn.close()
            return int(n or 0), float(mx or 0.0)
    
    @classmethod
    def remove_missing_under(cls, root: str):
        """존재하지 않는 파일 정리"""
        root = normalize_path(root)
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, path FROM files WHERE path LIKE ?;", (f"{root}%",))
        rows = cur.fetchall()
        removed = 0
        for fid, fpath in rows:
            if not Path(fpath).exists():
                cur.execute("DELETE FROM files WHERE id=?;", (fid,))
                removed += 1
        conn.commit()
        conn.close()
        return removed
    
    @classmethod
    def ensure_tag(cls, name: str):
        """태그 생성 또는 조회"""
        name = (name or "").strip()
        if not name: return None
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(ord),0)+1 FROM tags;")
        nxt = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO tags(name,ord) VALUES(?,?);", (name, nxt))
        cur.execute("SELECT id FROM tags WHERE name=?;", (name,))
        row = cur.fetchone()
        conn.commit()
        conn.close()
        return row[0] if row else None
    
    @classmethod
    def list_files(cls, search_text, tag_ids, only_tagged, root_prefix=None):
        """파일 목록 조회"""
        params, where, joins = [], [], []
        
        if search_text:
            where.append("f.path LIKE ?")
            params.append(f"%{search_text}%")
        if only_tagged:
            where.append("EXISTS (SELECT 1 FROM file_tags x WHERE x.file_id=f.id)")
        if tag_ids:
            for i, tid in enumerate(tag_ids):
                joins.append(f"JOIN file_tags ft{i} ON ft{i}.file_id=f.id AND ft{i}.tag_id=?")
                params.insert(0, tid)
        if root_prefix:
            where.append("f.path LIKE ?")
            params.append(f"{normalize_path(root_prefix)}%")
        
        join_sql = " ".join(joins)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        
        sql = f"""SELECT f.id, f.path, f.size, f.mtime,
                         (SELECT GROUP_CONCAT(t.name, ', ')
                          FROM tags t JOIN file_tags ft ON ft.tag_id=t.id
                          WHERE ft.file_id=f.id) AS tags
                  FROM files f {join_sql} {where_sql}
                  ORDER BY f.path ASC;"""
        
        conn = cls.get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()
        return rows

# ========================= 파일 시스템 =========================
def show_in_explorer(path: str):
    """탐색기에서 파일 표시"""
    if sys.platform.startswith("win"):
        os.system(f'explorer /select,"{path}"')
    elif sys.platform == "darwin":
        os.system(f'open -R "{path}"')
    else:
        os.system(f'xdg-open "{os.path.dirname(path)}"')

def recycle_delete(path: str) -> bool:
    """휴지통으로 삭제"""
    try:
        if sys.platform.startswith("win"):
            class SHFILEOPSTRUCT(ctypes.Structure):
                _fields_ = [
                    ('hwnd', wintypes.HWND), ('wFunc', wintypes.UINT),
                    ('pFrom', wintypes.LPCWSTR), ('pTo', wintypes.LPCWSTR),
                    ('fFlags', ctypes.c_uint), ('fAnyOperationsAborted', wintypes.BOOL),
                    ('hNameMappings', ctypes.c_void_p), ('lpszProgressTitle', wintypes.LPCWSTR)
                ]
            shell = ctypes.windll.shell32
            pFrom = normalize_path(path) + '\0\0'
            op = SHFILEOPSTRUCT(0, 0x0003, pFrom, None, 0x0050, False, None, None)
            res = shell.SHFileOperationW(ctypes.byref(op))
            return res == 0 and not op.fAnyOperationsAborted
        else:
            os.remove(path)
            return True
    except:
        return False

# ========================= 태그 칩 렌더러 =========================
class TagChipsDelegate(QStyledItemDelegate):
    """태그 칩 렌더링"""
    
    def __init__(self, color_resolver, parent=None):
        super().__init__(parent)
        self._color_resolver = color_resolver
    
    def paint(self, painter, option, index):
        if index.column() != 3:
            return super().paint(painter, option, index)
        
        text = index.data(Qt.DisplayRole) or ""
        tags = [t.strip() for t in text.split(",") if t.strip()]
        
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        opt.text = ""
        style = (opt.widget.style() if opt.widget else QApplication.style())
        style.drawControl(QStyle.CE_ItemViewItem, opt, painter, opt.widget)
        
        if not tags: return
        
        painter.save()
        pen = painter.pen()
        pen.setWidth(2)
        painter.setPen(pen)
        
        fm = opt.fontMetrics
        x, y_center = opt.rect.x() + 6, opt.rect.y() + opt.rect.height() // 2
        pad_h, pad_v, spacing = 8, 4, 6
        max_x, hidden = opt.rect.right() - 6, 0
        
        for t in tags:
            chip_w = fm.horizontalAdvance(t) + pad_h * 2
            chip_h = fm.height() + pad_v * 2
            if x + chip_w > max_x:
                hidden += 1
                break
            rect = QRectF(x, y_center - chip_h / 2, chip_w, chip_h)
            try:
                bg = self._color_resolver(t)
                painter.setBrush(bg if isinstance(bg, QColor) else color_for_item(None, t))
            except:
                painter.setBrush(color_for_item(None, t))
            painter.drawRoundedRect(rect, 10, 10)
            painter.drawText(rect, Qt.AlignCenter, t)
            x += chip_w + spacing
        
        if hidden > 0:
            more = f"+{hidden}"
            chip_w = fm.horizontalAdvance(more) + pad_h * 2
            chip_h = fm.height() + pad_v * 2
            if x + chip_w <= max_x:
                rect = QRectF(x, y_center - chip_h / 2, chip_w, chip_h)
                painter.setBrush(QColor(229, 231, 235))
                painter.drawRoundedRect(rect, 10, 10)
                painter.drawText(rect, Qt.AlignCenter, more)
        
        painter.restore()

# ========================= 메인 UI =========================
class MainUI(QWidget):
    progress_tick = Signal(int, int)
    scan_started = Signal(int)
    scan_finished = Signal()
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(1320, 800)
        
        self.db = DBManager()
        self.root_dir = None
        self.root_filter = None
        self.selected_tag_ids = set()
        self.scan_running = False
        self.tag_color_by_name = {}
        
        self._setup_ui()
        self._connect_signals()
        self._restore_state()
        self._initial_load()
    
    def _setup_ui(self):
        """UI 구성"""
        # 상단
        top = QVBoxLayout()
        title = QLabel(APP_TITLE)
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 20pt; font-weight: 900; margin: 4px 0 8px 0;")
        top.addWidget(title)
        
        row1 = QHBoxLayout()
        self.root_path_edit = QLineEdit()
        self.root_path_edit.setReadOnly(True)
        self.root_path_edit.setPlaceholderText("루트를 선택하세요…")
        self.root_path_edit.setFixedHeight(EDIT_H)
        self.root_path_edit.setStyleSheet("font-size:12pt;")
        
        self.btn_pick = self._create_button("루트 선택")
        self.btn_index = self._create_button("선택된 루트 색인하기", "primaryBtn")
        
        self.count_lbl = QLabel("결과: 0건")
        self.count_lbl.setStyleSheet("font-size:12pt; font-weight:800; padding-left:8px;")
        
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(10)
        
        self.search = QLineEdit()
        self.search.setPlaceholderText("파일명 검색…")
        self.search.setFixedHeight(EDIT_H)
        self.search.setFixedWidth(SEARCH_W)
        self.search.setStyleSheet("font-size:12pt;")
        
        self.btn_search = self._create_button("검색", width=SEARCH_BTN_W)
        
        row1.addWidget(self.root_path_edit, 1)
        row1.addWidget(self.btn_pick)
        row1.addWidget(self.btn_index)
        row1.addSpacing(8)
        row1.addWidget(self.count_lbl)
        row1.addSpacing(8)
        row1.addWidget(self.progress, 1)
        row1.addStretch(1)
        row1.addWidget(self.search)
        row1.addWidget(self.btn_search)
        top.addLayout(row1)
        
        # 좌우 패널
        left = self._create_left_panel()
        right = self._create_right_panel()
        
        # 중앙 테이블
        self.table = self._create_table()
        
        splitter = QSplitter()
        splitter.addWidget(left)
        splitter.addWidget(self.table)
        splitter.addWidget(right)
        splitter.setSizes([200, 960, 200])
        splitter.setStretchFactor(1, 1)
        for i in range(3):
            splitter.setCollapsible(i, False)
        
        main = QVBoxLayout()
        main.addLayout(top)
        main.addWidget(splitter, 1)
        self.setLayout(main)
        
        self._apply_styles()
    
    def _create_button(self, text, obj_name=None, width=None):
        """버튼 생성 헬퍼"""
        btn = QPushButton(text)
        btn.setFixedHeight(BTN_H)
        if width: btn.setFixedWidth(width)
        btn.setStyleSheet("font-size:12pt; font-weight:700;")
        if obj_name: btn.setObjectName(obj_name)
        return btn
    
    def _create_left_panel(self):
        """좌측 태그 패널"""
        left_box = QVBoxLayout()
        self.chk_only_tagged = QCheckBox("(체크)태그 파일 보기")
        left_box.addWidget(self.chk_only_tagged)
        left_box.addWidget(QLabel("---[태그]---"))
        
        self.tag_list = QListWidget()
        self.tag_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tag_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        left_box.addWidget(self.tag_list, 1)
        
        # 태그 버튼들
        btn_row1 = QHBoxLayout()
        self.btn_tag_up = self._create_button("▲ 위로")
        self.btn_tag_down = self._create_button("▼ 아래로")
        btn_row1.addWidget(self.btn_tag_up)
        btn_row1.addWidget(self.btn_tag_down)
        left_box.addLayout(btn_row1)
        
        self.btn_rename_tag = self._create_button("태그 이름 변경")
        left_box.addWidget(self.btn_rename_tag)
        
        self.new_tag = QLineEdit()
        self.new_tag.setPlaceholderText("새 태그 입력 후 추가")
        self.new_tag.setFixedHeight(EDIT_H)
        left_box.addWidget(self.new_tag)
        
        btn_row2 = QHBoxLayout()
        self.btn_add_tag = self._create_button("태그 추가", "primaryBtn")
        self.btn_del_tag = self._create_button("태그 삭제")
        for b in (self.btn_add_tag, self.btn_del_tag):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn_row2.addWidget(self.btn_add_tag)
        btn_row2.addWidget(self.btn_del_tag)
        left_box.addLayout(btn_row2)
        
        left = QWidget()
        left.setLayout(left_box)
        left.setMinimumWidth(190)
        return left
    
    def _create_right_panel(self):
        """우측 루트/태그 패널"""
        right_box = QVBoxLayout()
        right_box.setContentsMargins(6, 6, 6, 6)
        right_box.setSpacing(6)
        
        self.btn_rescan_all = self._create_button("색인 전체 재스캔", "primaryBtn")
        right_box.addWidget(self.btn_rescan_all)
        
        right_box.addWidget(QLabel("--[색인된 경로]---"))
        
        self.root_list = QListWidget()
        self.root_list.setFixedHeight(230)
        self.root_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.root_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        right_box.addWidget(self.root_list)
        
        btn_row = QHBoxLayout()
        self.btn_rescan_root = self._create_button("경로 재스캔", "primaryBtn")
        self.btn_remove_root = self._create_button("제거")
        for b in (self.btn_rescan_root, self.btn_remove_root):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn_row.addWidget(self.btn_rescan_root)
        btn_row.addWidget(self.btn_remove_root)
        right_box.addLayout(btn_row)
        
        # 바로가기 버튼
        link_row = QHBoxLayout()
        self.btn_school_neis = QPushButton("학교NEIS")
        self.btn_external_evpn = QPushButton("외부EVPN")
        for b in (self.btn_school_neis, self.btn_external_evpn):
            b.setFixedHeight(BTN_H)
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.btn_school_neis.setStyleSheet(
            "background:#DC2626; color:#FFF; border:2px solid #000; border-radius:10px; font-weight:900;")
        self.btn_external_evpn.setStyleSheet(
            "background:#16A34A; color:#FFF; border:2px solid #000; border-radius:10px; font-weight:900;")
        link_row.addWidget(self.btn_school_neis)
        link_row.addWidget(self.btn_external_evpn)
        right_box.addLayout(link_row)
        
        right_box.addSpacing(8)
        right_box.addWidget(QLabel("--[선택 파일의 태그]---"))
        
        self.sel_tags = QListWidget()
        self.sel_tags.setSelectionMode(QAbstractItemView.MultiSelection)
        self.sel_tags.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        right_box.addWidget(self.sel_tags, 1)
        
        self.combo_tag = QComboBox()
        self.combo_tag.setEditable(True)
        self.combo_tag.setFixedHeight(EDIT_H)
        right_box.addWidget(self.combo_tag)
        
        assign_row = QHBoxLayout()
        self.btn_assign = self._create_button("태그 붙이기", "primaryBtn")
        self.btn_untag = self._create_button("떼기")
        for b in (self.btn_assign, self.btn_untag):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        assign_row.addWidget(self.btn_assign)
        assign_row.addWidget(self.btn_untag)
        right_box.addLayout(assign_row)
        
        right = QWidget()
        right.setLayout(right_box)
        right.setMinimumWidth(190)
        return right
    
    def _create_table(self):
        """파일 테이블 생성"""
        table = QTableWidget(0, 6)
        table.setObjectName("fileTable")
        table.setHorizontalHeaderLabels(["파일", "크기", "수정시각", "태그", "위치", "ID"])
        table.setColumnHidden(5, True)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        table.setAlternatingRowColors(True)
        table.setSortingEnabled(True)
        table.setShowGrid(True)
        
        # 컬럼 너비
        for i, w in enumerate([400, 80, 140, 240, 420]):
            table.setColumnWidth(i, w)
        
        table.setContextMenuPolicy(Qt.CustomContextMenu)
        table.setMouseTracking(True)
        table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        
        # 헤더 설정
        table.verticalHeader().setDefaultSectionSize(29)
        table.horizontalHeader().setFixedHeight(31)
        table.horizontalHeader().setObjectName("fileTableH")
        table.verticalHeader().setObjectName("fileTableV")
        
        # 태그 칩 델리게이트
        self.tag_delegate = TagChipsDelegate(
            lambda name: self.tag_color_by_name.get(name, color_for_item(None, name)), table)
        table.setItemDelegateForColumn(3, self.tag_delegate)
        
        return table
    
    def _apply_styles(self):
        """스타일시트 적용"""
        self.setStyleSheet("""
        QWidget {background:#F3F4F6; color:#111827; font-size:12pt; font-weight:800;}
        QLineEdit, QComboBox, QListWidget, QTableWidget {
            background:#FFFFFF; color:#111827; border:1px solid #000000; border-radius:10px; font-weight:800;}
        QLineEdit:focus, QComboBox:focus {border:2px solid #000000; background:#FFFFFF;}
        #fileTable {font-size:10pt; border-radius:10px; gridline-color:#FFFFFF;}
        QHeaderView::section {
            background:#111827; color:#ffffff; padding:2px 8px; border:0; font-weight:900; font-size:12pt;
            border-right:1px solid rgba(255,255,255,0.06);}
        QHeaderView::section:last {border-right:0;}
        QTableWidget {alternate-background-color:#F7F7F8;}
        QTableWidget::item:selected, QListWidget::item:selected {background:#FDE68A; color:#111111;}
        QHeaderView#fileTableH::section {
            background:#111827; color:#ffffff; padding:2px 8px; border:0; border-right:2px solid #F59E0B;}
        QHeaderView#fileTableH::section:last {border-right:0;}
        QHeaderView#fileTableV::section {
            background:#111827; color:#ffffff; padding:2px 6px; border:0; border-bottom:1px solid #F59E0B;}
        QTableCornerButton::section {
            background:#111827; border:0; border-right:2px solid #F59E0B; border-bottom:1px solid #F59E0B;}
        QPushButton {
            background:#E5E7EB; border:2px solid #000000; padding:2px 8px;
            border-radius:10px; font-size:12pt; font-weight:800;}
        QPushButton:hover {background:#D1D5DB;}
        QPushButton:disabled {background:#E5E7EB; color:#9CA3AF; border-color:#000000;}
        QPushButton#primaryBtn {background:#2563EB; color:#FFFFFF; border:2px solid #000000; padding:2px 8px;}
        QPushButton#primaryBtn:hover {background:#1D4ED8; border:2px solid #000000;}
        QCheckBox {font-size:12pt; font-weight:800;}
        QCheckBox::indicator {
            width:18px; height:18px; border:2px solid #000000; background:#FFFFFF; border-radius:4px;}
        QCheckBox::indicator:checked {background:#DC2626; border:2px solid #000000;}
        QCheckBox::indicator:unchecked:hover {background:#FEE2E2;}
        QProgressBar {border:2px solid #000000; border-radius:6px; height:10px; text-align:center;}
        QProgressBar::chunk {background:#2563EB; border-radius:6px;}
        QScrollBar:vertical {background:#202124; width:16px; margin:0;}
        QScrollBar::handle:vertical {background:#3D3D3D; border-radius:6px; min-height:32px;}
        QScrollBar::handle:vertical:hover {background:#505050;}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {height:0;}
        QScrollBar:horizontal {background:#202124; height:16px; margin:0;}
        QScrollBar::handle:horizontal {background:#3D3D3D; border-radius:6px; min-width:32px;}
        QScrollBar::handle:horizontal:hover {background:#505050;}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {width:0;}
        QTableWidget#fileTable::item:hover {background:#DC2626; color:#FFFFFF;}
        QTableWidget#fileTable::item:selected:hover {background:#DC2626; color:#FFFFFF;}
        """)
    
    def _connect_signals(self):
        """시그널 연결"""
        # 루트/색인
        self.btn_pick.clicked.connect(self.pick_root)
        self.btn_index.clicked.connect(self.index_selected_root)
        self.btn_rescan_root.clicked.connect(self.rescan_selected_root)
        self.btn_remove_root.clicked.connect(self.remove_selected_root)
        self.btn_rescan_all.clicked.connect(self.rescan_all_roots)
        
        # 태그
        self.btn_add_tag.clicked.connect(self.add_tag)
        self.btn_del_tag.clicked.connect(self.delete_selected_tags)
        self.btn_tag_up.clicked.connect(lambda: self.move_tag(-1))
        self.btn_tag_down.clicked.connect(lambda: self.move_tag(1))
        self.btn_rename_tag.clicked.connect(self.rename_selected_tag)
        self.tag_list.itemDoubleClicked.connect(self.rename_tag_inline)
        self.tag_list.itemClicked.connect(self.on_tag_clicked)
        
        # 검색/필터
        self.btn_search.clicked.connect(self.refresh_files_and_save)
        self.search.returnPressed.connect(self.refresh_files_and_save)
        self.chk_only_tagged.stateChanged.connect(self.on_only_tagged_toggled)
        
        # 테이블/파일
        self.root_list.itemClicked.connect(self.on_root_clicked)
        self.table.itemSelectionChanged.connect(self.refresh_selected_file_tags)
        self.table.itemDoubleClicked.connect(self.open_file)
        self.table.customContextMenuRequested.connect(self.on_table_context_menu)
        
        # 파일 태그
        self.btn_assign.clicked.connect(self.assign_tag_to_selected)
        self.btn_untag.clicked.connect(self.untag_selected_from_selected_files)
        
        # 바로가기
        self.btn_school_neis.clicked.connect(
            lambda: webbrowser.open("https://goe.eduptl.kr/bpm_lgn_lg00_001.do"))
        self.btn_external_evpn.clicked.connect(
            lambda: webbrowser.open("https://evpn.goe.go.kr/custom/index.html"))
        
        # 진행 표시
        self.scan_started.connect(self._on_scan_started)
        self.progress_tick.connect(self._on_progress_tick)
        self.scan_finished.connect(self._on_scan_finished)
        
        # 단축키
        QShortcut(QKeySequence("F5"), self).activated.connect(self.refresh_all_counts)
    
    def _restore_state(self):
        """상태 복원"""
        self.root_dir = self.db.get_setting("last_root", "") or None
        if self.root_dir:
            self.root_path_edit.setText(self.root_dir)
        self.search.setText(self.db.get_setting("last_search", ""))
        self.chk_only_tagged.setChecked(self.db.get_setting("last_only_tagged", "0") == "1")
        self.update_checkbox_style()
    
    def _initial_load(self):
        """초기 로드"""
        self.refresh_roots_panel()
        self.refresh_tags()
        self.refresh_files()
        
        # 자동 재스캔 또는 루트 선택
        from PySide6.QtCore import QTimer
        if self._list_roots():
            QTimer.singleShot(0, self.rescan_all_roots)
        else:
            QTimer.singleShot(0, self.pick_root)
    
    # ==================== DB 헬퍼 메서드 ====================
    def _list_roots(self):
        """루트 목록 조회"""
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT path FROM roots ORDER BY path;")
        rows = [normalize_path(r[0]) for r in cur.fetchall()]
        conn.close()
        return rows
    
    def _add_root(self, path: str):
        """루트 추가"""
        path = normalize_path(path)
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("""INSERT INTO roots(path,last_scanned) VALUES(?,strftime('%s','now'))
                       ON CONFLICT(path) DO UPDATE SET last_scanned=strftime('%s','now');""", (path,))
        conn.commit()
        conn.close()
    
    def _remove_root(self, path: str):
        """루트 제거"""
        path = normalize_path(path)
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM roots WHERE path=?;", (path,))
        cur.execute("DELETE FROM files WHERE path LIKE ?;", (f"{path}%",))
        conn.commit()
        conn.close()
    
    def _list_tags(self):
        """태그 목록"""
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name, ord FROM tags ORDER BY ord ASC, name ASC;")
        rows = cur.fetchall()
        conn.close()
        return rows
    
    def _delete_tags(self, tag_ids):
        """태그 삭제"""
        if not tag_ids: return
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.executemany("DELETE FROM tags WHERE id=?;", [(tid,) for tid in tag_ids])
        conn.commit()
        conn.close()
    
    def _count_files_by_tag(self):
        """태그별 파일 개수"""
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, COALESCE(COUNT(ft.file_id),0) AS cnt
            FROM tags t LEFT JOIN file_tags ft ON ft.tag_id=t.id
            GROUP BY t.id ORDER BY t.ord, t.name;
        """)
        rows = cur.fetchall()
        conn.close()
        return {tid: cnt for tid, cnt in rows}
    
    def _list_file_tags(self, file_id: int):
        """파일의 태그 목록"""
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.name FROM tags t
            JOIN file_tags ft ON ft.tag_id=t.id
            WHERE ft.file_id=? ORDER BY t.ord, t.name;""", (file_id,))
        rows = cur.fetchall()
        conn.close()
        return rows
    
    # ==================== UI 업데이트 ====================
    def refresh_roots_panel(self):
        """루트 패널 새로고침"""
        self.root_list.clear()
        all_item = QListWidgetItem("전체 경로 보기")
        all_item.setData(Qt.UserRole, None)
        all_item.setToolTip("등록된 모든 경로 합산 보기")
        self.root_list.addItem(all_item)
        
        for root in sorted(set(self._list_roots()), key=str.lower):
            cnt, _ = self.db.count_and_stats(root)
            it = QListWidgetItem(f"{root} ({cnt:,})")
            it.setData(Qt.UserRole, root)
            it.setBackground(QBrush(color_for_item(root)))
            it.setToolTip(root)
            self.root_list.addItem(it)
    
    def refresh_tags(self):
        """태그 패널 새로고침"""
        self.tag_list.clear()
        total_cnt, _ = self.db.count_and_stats()
        
        self.tag_color_by_name = {}
        
        all_item = QListWidgetItem(f"전체 파일 ({total_cnt:,})")
        all_item.setData(Qt.UserRole, None)
        all_item.setBackground(QBrush(QColor(235, 235, 235)))
        self.tag_list.addItem(all_item)
        
        by_tag = self._count_files_by_tag()
        self.combo_tag.clear()
        
        for tid, name, _ord in self._list_tags():
            cnt = by_tag.get(tid, 0)
            it = QListWidgetItem(f"{name} ({cnt:,})")
            it.setData(Qt.UserRole, tid)
            col = color_for_item(tid, name)
            it.setBackground(QBrush(col))
            self.tag_list.addItem(it)
            
            self.combo_tag.addItem(name, userData=tid)
            idx = self.combo_tag.count() - 1
            self.combo_tag.setItemData(idx, QBrush(col), Qt.BackgroundRole)
            
            self.tag_color_by_name[name] = col
        
        self.tag_list.setCurrentRow(0)
    
    def refresh_files(self):
        """파일 목록 새로고침"""
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        
        rows = self.db.list_files(
            self.search.text().strip(),
            list(self.selected_tag_ids),
            self.chk_only_tagged.isChecked(),
            self.root_filter
        )
        self.count_lbl.setText(f"결과: {len(rows):,}건")
        
        for fid, path, size, mtime, tag_text in rows:
            r = self.table.rowCount()
            self.table.insertRow(r)
            
            fname = os.path.basename(path)
            fdir = os.path.dirname(path)
            
            it_file = QTableWidgetItem(fname)
            it_file.setData(Qt.UserRole, path)
            it_file.setToolTip(path)
            
            it_size = QTableWidgetItem(format_size_explorer(size))
            it_size.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            
            it_mtim = QTableWidgetItem(datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S"))
            it_tags = QTableWidgetItem(tag_text or "")
            it_dir = QTableWidgetItem(fdir)
            it_dir.setToolTip(path)
            it_id = QTableWidgetItem(str(fid))
            
            # 첫 태그 색상 적용
            if tag_text:
                first_tag = (tag_text.split(",")[0] or "").strip()
                if first_tag:
                    col = self.tag_color_by_name.get(first_tag, color_for_item(None, first_tag))
                    it_file.setBackground(QBrush(col))
            
            self.table.setItem(r, 0, it_file)
            self.table.setItem(r, 1, it_size)
            self.table.setItem(r, 2, it_mtim)
            self.table.setItem(r, 3, it_tags)
            self.table.setItem(r, 4, it_dir)
            self.table.setItem(r, 5, it_id)
        
        self.table.setSortingEnabled(True)
        self.table.sortItems(2, Qt.DescendingOrder)
    
    def refresh_all_counts(self):
        """전체 카운트 새로고침"""
        cur_item = self.root_list.currentItem()
        cur_path = cur_item.data(Qt.UserRole) if cur_item else None
        vpos = self.root_list.verticalScrollBar().value()
        
        self.refresh_roots_panel()
        
        if cur_path is None:
            self.root_list.setCurrentRow(0)
        else:
            for i in range(self.root_list.count()):
                if self.root_list.item(i).data(Qt.UserRole) == cur_path:
                    self.root_list.setCurrentRow(i)
                    break
        
        self.root_list.verticalScrollBar().setValue(vpos)
        self.refresh_tags()
        self.refresh_files()
    
    def refresh_files_and_save(self):
        """검색어 저장 후 새로고침"""
        self.db.set_setting("last_search", self.search.text().strip())
        self.db.set_setting("last_only_tagged", "1" if self.chk_only_tagged.isChecked() else "0")
        self.refresh_files()
    
    # ==================== 이벤트 핸들러 ====================
    def on_tag_clicked(self, item: QListWidgetItem):
        """태그 클릭"""
        tid = item.data(Qt.UserRole)
        self.selected_tag_ids = set() if tid is None else {tid}
        self.refresh_files()
    
    def on_root_clicked(self, item: QListWidgetItem):
        """루트 클릭"""
        path = item.data(Qt.UserRole)
        self.root_filter = None if path is None else path
        self.refresh_files()
    
    def on_only_tagged_toggled(self, _state):
        """태그 필터 토글"""
        self.update_checkbox_style()
        self.refresh_files_and_save()
    
    def update_checkbox_style(self):
        """체크박스 스타일 업데이트"""
        if self.chk_only_tagged.isChecked():
            self.chk_only_tagged.setStyleSheet("color:#b00020; font-weight:900;")
        else:
            self.chk_only_tagged.setStyleSheet("color:#1e1f23; font-weight:800;")
    
    def on_table_context_menu(self, pos):
        """테이블 컨텍스트 메뉴"""
        if not self.table.itemAt(pos):
            return
        
        menu = QMenu(self)
        act_open = menu.addAction("열기")
        act_show = menu.addAction("폴더에서 보기")
        act_rename = menu.addAction("이름 바꾸기")
        act_delete = menu.addAction("삭제(휴지통)")
        
        act = menu.exec_(self.table.viewport().mapToGlobal(pos))
        if not act:
            return
        
        if act == act_open:
            ps = self._get_selected_paths()
            if ps:
                self._open_path(ps[0])
        elif act == act_show:
            for p in self._get_selected_paths():
                show_in_explorer(p)
        elif act == act_rename:
            self.rename_selected_file()
        elif act == act_delete:
            self.delete_selected_files()
    
    def open_file(self, item):
        """파일 열기"""
        if item.column() != 0:
            return
        full_path = item.data(Qt.UserRole)
        if full_path:
            self._open_path(full_path)
    
    def _open_path(self, full_path: str):
        """경로 열기"""
        try:
            if sys.platform.startswith("win"):
                os.startfile(full_path)
            elif sys.platform == "darwin":
                os.system(f'open "{full_path}"')
            else:
                os.system(f'xdg-open "{full_path}"')
        except Exception as e:
            QMessageBox.warning(self, "오류", f"파일 열기 실패:\n{e}")
    
    # ==================== 진행 표시 ====================
    def _on_scan_started(self, total: int):
        """스캔 시작"""
        self.count_lbl.setText(f"색인 중… (0 / {total:,})")
        self.progress.setVisible(True)
        self.progress.setMaximum(max(total, 1))
        self.progress.setValue(0)
    
    def _on_progress_tick(self, processed: int, total: int):
        """진행 표시"""
        self.progress.setValue(processed)
        self.count_lbl.setText(f"색인 중… ({processed:,} / {total:,})")
    
    def _on_scan_finished(self):
        """스캔 완료"""
        self.scan_running = False
        self.progress.setVisible(False)
        self.root_path_edit.clear()
        self.refresh_all_counts()
        self.search.setFocus()
    
    # ==================== 태그 조작 ====================
    def add_tag(self):
        """태그 추가"""
        name = self.new_tag.text().strip()
        if not name:
            return
        self.db.ensure_tag(name)
        self.new_tag.clear()
        self.refresh_tags()
    
    def delete_selected_tags(self):
        """선택 태그 삭제"""
        items = [it for it in self.tag_list.selectedItems() if it.data(Qt.UserRole) is not None]
        if not items:
            QMessageBox.information(self, "안내", "삭제할 태그를 선택하세요.")
            return
        
        names = [it.text().rsplit(" (", 1)[0] for it in items]
        preview = ", ".join(names[:10]) + ("…" if len(names) > 10 else "")
        
        if QMessageBox.question(self, "태그 삭제",
                                f"{len(items)}개 태그를 삭제할까요?\n{preview}") \
                != QMessageBox.StandardButton.Yes:
            return
        
        tids = [it.data(Qt.UserRole) for it in items]
        self._delete_tags(tids)
        self.refresh_all_counts()
    
    def move_tag(self, delta: int):
        """태그 순서 이동"""
        cur = self.tag_list.currentItem()
        if not cur:
            return
        tid = cur.data(Qt.UserRole)
        if tid is None:
            return
        
        conn = self.db.get_conn()
        c = conn.cursor()
        c.execute("SELECT ord FROM tags WHERE id=?;", (tid,))
        row = c.fetchone()
        if not row:
            conn.close()
            return
        
        cur_ord = row[0]
        if delta < 0:
            c.execute("SELECT id, ord FROM tags WHERE ord<? ORDER BY ord DESC LIMIT 1;", (cur_ord,))
        else:
            c.execute("SELECT id, ord FROM tags WHERE ord>? ORDER BY ord ASC LIMIT 1;", (cur_ord,))
        
        nb = c.fetchone()
        if not nb:
            conn.close()
            return
        
        nb_id, nb_ord = nb
        c.execute("UPDATE tags SET ord=? WHERE id=?;", (nb_ord, tid))
        c.execute("UPDATE tags SET ord=? WHERE id=?;", (cur_ord, nb_id))
        conn.commit()
        conn.close()
        
        self.refresh_tags()
        for i in range(self.tag_list.count()):
            if self.tag_list.item(i).data(Qt.UserRole) == tid:
                self.tag_list.setCurrentRow(i)
                break
    
    def rename_selected_tag(self):
        """선택 태그 이름 변경"""
        cur = self.tag_list.currentItem()
        if not cur or cur.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "이름을 바꿀 태그를 선택하세요.")
            return
        
        old_tid = cur.data(Qt.UserRole)
        old_name = cur.text().rsplit(" (", 1)[0]
        new_name, ok = QInputDialog.getText(self, "태그 이름 변경", "새 태그 이름:", text=old_name)
        
        if ok:
            self._rename_or_merge_tag(old_tid, new_name.strip())
    
    def rename_tag_inline(self, item: QListWidgetItem):
        """태그 더블클릭 이름 변경"""
        tid = item.data(Qt.UserRole)
        if tid is None:
            return
        
        old_name = item.text().rsplit(" (", 1)[0]
        new_name, ok = QInputDialog.getText(self, "태그 이름 변경", "새 태그 이름:", text=old_name)
        
        if ok:
            self._rename_or_merge_tag(tid, new_name.strip())
    
    def _rename_or_merge_tag(self, old_tid: int, new_name: str):
        """태그 이름 변경 또는 병합"""
        if not new_name:
            return
        
        conn = self.db.get_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM tags WHERE name=?;", (new_name,))
        row = c.fetchone()
        
        if row:
            new_tid = row[0]
            if new_tid == old_tid:
                conn.close()
                return
            
            # 병합
            c.execute("INSERT OR IGNORE INTO file_tags(file_id, tag_id) "
                      "SELECT file_id, ? FROM file_tags WHERE tag_id=?;", (new_tid, old_tid))
            c.execute("DELETE FROM file_tags WHERE tag_id=?;", (old_tid,))
            c.execute("DELETE FROM tags WHERE id=?;", (old_tid,))
            conn.commit()
            conn.close()
            QMessageBox.information(self, "안내", f"동일 이름이 있어 태그를 병합했습니다: {new_name}")
        else:
            try:
                c.execute("UPDATE tags SET name=? WHERE id=?;", (new_name, old_tid))
                conn.commit()
            except sqlite3.IntegrityError:
                conn.close()
                QMessageBox.warning(self, "오류", "태그 이름이 중복되어 변경할 수 없습니다.")
                return
            conn.close()
        
        self.refresh_all_counts()
    
    # ==================== 파일 태그 조작 ====================
    def refresh_selected_file_tags(self):
        """선택 파일의 태그 표시"""
        self.sel_tags.clear()
        ids = self._selected_file_ids()
        if len(ids) != 1:
            return
        
        for tid, name in self._list_file_tags(ids[0]):
            it = QListWidgetItem(name)
            it.setData(Qt.UserRole, tid)
            it.setBackground(QBrush(color_for_item(tid, name)))
            self.sel_tags.addItem(it)
    
    def assign_tag_to_selected(self):
        """선택 파일에 태그 할당"""
        name = self.combo_tag.currentText().strip()
        if not name:
            return
        
        tid = self.db.ensure_tag(name)
        ids = self._selected_file_ids()
        
        if not ids:
            QMessageBox.information(self, "안내", "파일을 먼저 선택하세요.")
            return
        
        conn = self.db.get_conn()
        cur = conn.cursor()
        for fid in ids:
            cur.execute("INSERT OR IGNORE INTO file_tags(file_id, tag_id) VALUES(?, ?);", (fid, tid))
        conn.commit()
        conn.close()
        
        self.refresh_all_counts()
    
    def untag_selected_from_selected_files(self):
        """선택 파일에서 태그 제거"""
        ids = self._selected_file_ids()
        if len(ids) != 1:
            QMessageBox.information(self, "안내", "오른쪽 목록은 단일 파일 선택 시만 조작됩니다.")
            return
        
        fid = ids[0]
        selected = self.sel_tags.selectedItems()
        if not selected:
            return
        
        conn = self.db.get_conn()
        cur = conn.cursor()
        for it in selected:
            cur.execute("DELETE FROM file_tags WHERE file_id=? AND tag_id=?;",
                       (fid, it.data(Qt.UserRole)))
        conn.commit()
        conn.close()
        
        self.refresh_all_counts()
    
    # ==================== 파일 조작 ====================
    def rename_selected_file(self):
        """파일 이름 변경"""
        ps = self._get_selected_paths()
        if len(ps) != 1:
            QMessageBox.information(self, "안내", "이름 변경은 한 개의 파일만 선택하세요.")
            return
        
        old = ps[0]
        new_name, ok = QInputDialog.getText(self, "이름 바꾸기", "새 파일명:",
                                            text=os.path.basename(old))
        if not ok or not new_name.strip():
            return
        
        new_path = os.path.join(os.path.dirname(old), new_name.strip())
        try:
            os.rename(old, new_path)
        except Exception as e:
            QMessageBox.warning(self, "오류", f"이름 바꾸기 실패:\n{e}")
            return
        
        # DB 업데이트
        conn = self.db.get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE files SET path=? WHERE path=?;",
                    (normalize_path(new_path), normalize_path(old)))
        conn.commit()
        conn.close()
        
        self.refresh_all_counts()
    
    def delete_selected_files(self):
        """파일 삭제"""
        ps = self._get_selected_paths()
        if not ps:
            return
        
        if QMessageBox.question(self, "삭제 확인",
                                f"{len(ps)}개 파일을 삭제(휴지통)할까요?") \
                != QMessageBox.StandardButton.Yes:
            return
        
        conn = self.db.get_conn()
        cur = conn.cursor()
        for p in ps:
            if recycle_delete(p):
                cur.execute("DELETE FROM files WHERE path=?;", (normalize_path(p),))
        conn.commit()
        conn.close()
        
        self.refresh_all_counts()
    
    # ==================== 루트/스캔 ====================
    def pick_root(self):
        """루트 선택"""
        if self._check_busy("루트 선택"):
            return
        
        d = QFileDialog.getExistingDirectory(self, "루트 폴더 선택")
        if d:
            self.root_dir = normalize_path(d)
            self.root_path_edit.setText(self.root_dir)
            self.db.set_setting("last_root", self.root_dir)
            self._add_root(self.root_dir)
            self.refresh_roots_panel()
            self.root_filter = None
            self.root_list.setCurrentRow(0)
            self.refresh_tags()
            self.refresh_files()
    
    def index_selected_root(self):
        """선택된 루트 색인"""
        root = self.root_dir
        if not root:
            it = self.root_list.currentItem()
            if it and it.data(Qt.UserRole):
                root = it.data(Qt.UserRole)
        
        if not root:
            QMessageBox.information(self, "안내", "먼저 [루트 선택]으로 색인할 폴더를 지정하세요.")
            return
        
        if self._check_busy("색인"):
            return
        
        self.scan_running = True
        root = normalize_path(root)
        
        fs_total, fs_max_m = self.db.count_and_stats(root, is_disk=True)
        db_total, db_max_m = self.db.count_and_stats(root)
        
        if fs_total == db_total and (db_max_m >= fs_max_m):
            self.scan_finished.emit()
            return
        
        self.scan_started.emit(fs_total)
        step = max(1, fs_total // 100)
        
        def worker():
            processed = 0
            try:
                self._add_root(root)
                for dp, _, files in os.walk(root):
                    for fn in files:
                        full = os.path.join(dp, fn)
                        try:
                            self.db.upsert_file(full)
                        except:
                            pass
                        processed += 1
                        if (processed % step == 0) or (processed == fs_total):
                            self.progress_tick.emit(processed, fs_total)
                self.db.remove_missing_under(root)
            finally:
                self.scan_finished.emit()
        
        threading.Thread(target=worker, daemon=True).start()
    
    def rescan_selected_root(self):
        """선택 루트 재스캔"""
        if self._check_busy("재스캔"):
            return
        
        item = self.root_list.currentItem()
        if not item or item.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "재스캔할 경로를 선택하세요.")
            return
        
        root = item.data(Qt.UserRole)
        self.root_dir = root
        self.root_path_edit.setText(root)
        self.index_selected_root()
    
    def rescan_all_roots(self):
        """전체 루트 재스캔"""
        if self._check_busy("전체 재스캔"):
            return
        
        roots = self._list_roots()
        if not roots:
            QMessageBox.information(self, "안내", "재스캔할 경로가 없습니다.")
            return
        
        uniq = list(set(roots))
        total = sum(self.db.count_and_stats(r, is_disk=True)[0] for r in uniq)
        
        self.scan_running = True
        self.scan_started.emit(total if total > 0 else 1)
        
        def worker():
            processed = 0
            try:
                for root in uniq:
                    self._add_root(root)
                    for dp, _, files in os.walk(root):
                        for fn in files:
                            full = os.path.join(dp, fn)
                            try:
                                self.db.upsert_file(full)
                            except:
                                pass
                            processed += 1
                            if processed % 500 == 0 or processed == total:
                                self.progress_tick.emit(processed, total if total > 0 else 1)
                    self.db.remove_missing_under(root)
            finally:
                self.scan_finished.emit()
        
        threading.Thread(target=worker, daemon=True).start()
    
    def remove_selected_root(self):
        """선택 루트 제거"""
        if self._check_busy("제거"):
            return
        
        item = self.root_list.currentItem()
        if not item or item.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "제거할 경로를 선택하세요.")
            return
        
        root = item.data(Qt.UserRole)
        
        if QMessageBox.question(self, "경로 제거",
                                f"'{root}' 경로를 목록에서 제거하고 관련 파일 기록을 삭제할까요?") \
                != QMessageBox.StandardButton.Yes:
            return
        
        self._remove_root(root)
        
        if self.root_filter == root:
            self.root_filter = None
        if self.root_path_edit.text() == root:
            self.root_path_edit.clear()
        if self.root_dir == root:
            self.root_dir = None
        
        self.refresh_roots_panel()
        self.root_list.setCurrentRow(0)
        self.refresh_files()
    
    # ==================== 헬퍼 메서드 ====================
    def _check_busy(self, purpose: str) -> bool:
        """작업 중 체크"""
        if self.scan_running:
            QMessageBox.information(self, "안내",
                                    f"현재 다른 작업이 진행 중입니다.\n({purpose})가 끝난 후 다시 시도하세요.")
            return True
        return False
    
    def _selected_file_ids(self):
        """선택된 파일 ID 목록"""
        ids = []
        if self.table.selectionModel():
            for idx in self.table.selectionModel().selectedRows():
                ids.append(int(self.table.item(idx.row(), 5).text()))
        return ids
    
    def _get_selected_paths(self):
        """선택된 파일 경로 목록"""
        paths = []
        if self.table.selectionModel():
            for idx in self.table.selectionModel().selectedRows():
                p = self.table.item(idx.row(), 0).data(Qt.UserRole)
                if p:
                    paths.append(p)
        return paths

# ========================= 엔트리포인트 =========================
def main():
    DBManager.init_db()
    app = QApplication(sys.argv)
    ui = MainUI()
    ui.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()