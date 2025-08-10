#!/usr/bin/env python3
"""
🕒 ETL Debrito - Task Timer com Tracking Temporal
=================================================

Timer básico para monitoramento de tarefas TDD com:
- Controle start/pause/stop
- Tracking estimado vs real 
- Integração com Epic structure
- Persistência SQLite para analytics

Uso:
    python -m tdah_tools.task_timer start 3.1a
    python -m tdah_tools.task_timer pause
    python -m tdah_tools.task_timer resume  
    python -m tdah_tools.task_timer stop
    python -m tdah_tools.task_timer status
    python -m tdah_tools.task_timer report
"""

import argparse
import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict


@dataclass
class TaskSession:
    """Representa uma sessão de trabalho em uma task."""
    task_id: str
    epic_id: str
    start_time: float
    end_time: Optional[float] = None
    pause_time: Optional[float] = None
    paused_duration: float = 0.0
    estimate_minutes: int = 10
    actual_seconds: Optional[float] = None
    status: str = "active"  # active, paused, completed
    
    def get_elapsed_seconds(self) -> float:
        """Retorna segundos decorridos (excluindo pausas)."""
        if self.status == "completed" and self.actual_seconds:
            return self.actual_seconds
        
        current_time = self.pause_time or time.time()
        if self.end_time:
            current_time = self.end_time
            
        return (current_time - self.start_time) - self.paused_duration
    
    def get_elapsed_minutes(self) -> float:
        """Retorna minutos decorridos."""
        return self.get_elapsed_seconds() / 60.0
    
    def get_accuracy_ratio(self) -> Optional[float]:
        """Retorna ratio accuracy (real/estimado). None se incompleto."""
        if self.status != "completed":
            return None
        return self.get_elapsed_minutes() / self.estimate_minutes


class TaskTimer:
    """Gerenciador principal do timer de tarefas."""
    
    def __init__(self, db_path: str = "task_timer.db"):
        self.db_path = Path(db_path)
        self.current_session: Optional[TaskSession] = None
        self.init_database()
        self._load_active_session()
        
    def init_database(self) -> None:
        """Inicializa schema SQLite para tracking."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    epic_id TEXT NOT NULL,
                    start_time REAL NOT NULL,
                    end_time REAL,
                    pause_time REAL,
                    paused_duration REAL DEFAULT 0.0,
                    estimate_minutes INTEGER NOT NULL,
                    actual_seconds REAL,
                    status TEXT DEFAULT 'active',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_task_sessions_task_id 
                ON task_sessions(task_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_task_sessions_epic_id 
                ON task_sessions(epic_id)
            """)
    
    def load_task_from_epic(self, task_id: str) -> Optional[Tuple[str, int]]:
        """Carrega epic_id e estimate_minutes a partir dos arquivos épicos."""
        for epic_file in Path(".").glob("epico_*.json"):
            try:
                with open(epic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                epic_data = data.get('epic', {})
                epic_id = str(epic_data.get('id', ''))
                
                for task_data in epic_data.get('tasks', []):
                    if task_data.get('id') == task_id:
                        estimate = task_data.get('estimate_minutes', 10)
                        return epic_id, estimate
                        
            except Exception as e:
                print(f"⚠️ Erro ao carregar {epic_file}: {e}")
        
        return None
    
    def _load_active_session(self) -> None:
        """Carrega sessão ativa do banco se existir."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT task_id, epic_id, start_time, pause_time, 
                       paused_duration, estimate_minutes, status
                FROM task_sessions 
                WHERE status IN ('active', 'paused')
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if row:
                task_id, epic_id, start_time, pause_time, paused_duration, estimate_minutes, status = row
                self.current_session = TaskSession(
                    task_id=task_id,
                    epic_id=epic_id,
                    start_time=start_time,
                    pause_time=pause_time,
                    paused_duration=paused_duration,
                    estimate_minutes=estimate_minutes,
                    status=status
                )
    
    def _save_current_session_state(self) -> None:
        """Salva ou atualiza estado da sessão atual."""
        if not self.current_session:
            return
            
        with sqlite3.connect(self.db_path) as conn:
            # Tentar atualizar existente
            cursor = conn.execute("""
                UPDATE task_sessions 
                SET pause_time = ?, paused_duration = ?, status = ?
                WHERE task_id = ? AND status IN ('active', 'paused')
            """, (
                self.current_session.pause_time,
                self.current_session.paused_duration,
                self.current_session.status,
                self.current_session.task_id
            ))
            
            # Se não existe, criar novo
            if cursor.rowcount == 0:
                conn.execute("""
                    INSERT INTO task_sessions 
                    (task_id, epic_id, start_time, pause_time, 
                     paused_duration, estimate_minutes, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.current_session.task_id,
                    self.current_session.epic_id,
                    self.current_session.start_time,
                    self.current_session.pause_time,
                    self.current_session.paused_duration,
                    self.current_session.estimate_minutes,
                    self.current_session.status
                ))
    
    def start_task(self, task_id: str) -> bool:
        """Inicia timer para uma task específica."""
        if self.current_session and self.current_session.status == "active":
            print(f"❌ Task {self.current_session.task_id} já está ativa!")
            print(f"   Use 'pause' ou 'stop' primeiro")
            return False
        
        # Carregar dados da task do épico
        task_info = self.load_task_from_epic(task_id)
        if not task_info:
            print(f"❌ Task '{task_id}' não encontrada nos épicos!")
            return False
        
        epic_id, estimate_minutes = task_info
        
        # Criar nova sessão
        self.current_session = TaskSession(
            task_id=task_id,
            epic_id=epic_id,
            start_time=time.time(),
            estimate_minutes=estimate_minutes
        )
        
        print(f"🏁 Timer iniciado para task {task_id}")
        print(f"   Epic: {epic_id}")
        print(f"   Estimativa: {estimate_minutes} min")
        print(f"   Início: {datetime.now().strftime('%H:%M:%S')}")
        
        # Salvar estado inicial
        self._save_current_session_state()
        
        return True
    
    def pause_task(self) -> bool:
        """Pausa task atual."""
        if not self.current_session or self.current_session.status != "active":
            print("❌ Nenhuma task ativa para pausar!")
            return False
        
        self.current_session.pause_time = time.time()
        self.current_session.status = "paused"
        
        elapsed_min = self.current_session.get_elapsed_minutes()
        print(f"⏸️ Task {self.current_session.task_id} pausada")
        print(f"   Tempo decorrido: {elapsed_min:.1f} min")
        
        # Salvar estado
        self._save_current_session_state()
        
        return True
    
    def resume_task(self) -> bool:
        """Retoma task pausada."""
        if not self.current_session or self.current_session.status != "paused":
            print("❌ Nenhuma task pausada para retomar!")
            return False
        
        if self.current_session.pause_time:
            pause_duration = time.time() - self.current_session.pause_time
            self.current_session.paused_duration += pause_duration
            self.current_session.pause_time = None
        
        self.current_session.status = "active"
        
        print(f"▶️ Task {self.current_session.task_id} retomada")
        
        # Salvar estado
        self._save_current_session_state()
        
        return True
    
    def stop_task(self) -> bool:
        """Para task atual e salva no banco."""
        if not self.current_session:
            print("❌ Nenhuma task ativa para parar!")
            return False
        
        # Calcular tempo final
        if self.current_session.status == "paused" and self.current_session.pause_time:
            # Se estava pausada, usar tempo de pausa como fim
            self.current_session.end_time = self.current_session.pause_time
        else:
            # Se estava ativa, usar tempo atual
            self.current_session.end_time = time.time()
        
        self.current_session.actual_seconds = self.current_session.get_elapsed_seconds()
        self.current_session.status = "completed"
        
        # Salvar no banco
        self._save_session(self.current_session)
        
        # Exibir resultado
        elapsed_min = self.current_session.get_elapsed_minutes()
        estimate_min = self.current_session.estimate_minutes
        accuracy = self.current_session.get_accuracy_ratio()
        
        print(f"🏁 Task {self.current_session.task_id} finalizada!")
        print(f"   Tempo real: {elapsed_min:.1f} min")
        print(f"   Estimativa: {estimate_min} min")
        
        if accuracy:
            if accuracy <= 1.1:  # Dentro de 10% da estimativa
                print(f"   Accuracy: {accuracy:.2f} ✅ Excelente!")
            elif accuracy <= 1.5:
                print(f"   Accuracy: {accuracy:.2f} 🟡 Razoável")
            else:
                print(f"   Accuracy: {accuracy:.2f} 🔴 Precisa melhorar")
        
        self.current_session = None
        return True
    
    def get_status(self) -> None:
        """Mostra status atual do timer."""
        if not self.current_session:
            print("⏹️ Nenhuma task ativa")
            return
        
        elapsed_min = self.current_session.get_elapsed_minutes()
        estimate_min = self.current_session.estimate_minutes
        progress = (elapsed_min / estimate_min) * 100
        
        status_emoji = "⏸️" if self.current_session.status == "paused" else "▶️"
        
        print(f"{status_emoji} Task: {self.current_session.task_id}")
        print(f"   Epic: {self.current_session.epic_id}")
        print(f"   Tempo: {elapsed_min:.1f}/{estimate_min} min ({progress:.1f}%)")
        print(f"   Status: {self.current_session.status}")
        
        if progress > 100:
            print(f"   ⚠️ Excedeu estimativa em {progress-100:.1f}%")
    
    def _save_session(self, session: TaskSession) -> None:
        """Salva sessão no banco de dados."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO task_sessions 
                (task_id, epic_id, start_time, end_time, pause_time, 
                 paused_duration, estimate_minutes, actual_seconds, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session.task_id, session.epic_id, session.start_time,
                session.end_time, session.pause_time, session.paused_duration,
                session.estimate_minutes, session.actual_seconds, session.status
            ))
    
    def generate_report(self, days: int = 7) -> None:
        """Gera relatório de accuracy das últimas sessões."""
        cutoff = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT task_id, epic_id, estimate_minutes, actual_seconds
                FROM task_sessions 
                WHERE status = 'completed' 
                  AND created_at >= ?
                ORDER BY created_at DESC
            """, (cutoff,))
            
            sessions = cursor.fetchall()
        
        if not sessions:
            print(f"📊 Nenhuma task completada nos últimos {days} dias")
            return
        
        print(f"📊 Relatório de Accuracy ({days} dias)")
        print("=" * 50)
        
        total_accuracy = 0
        accurate_count = 0  # Dentro de 10% da estimativa
        
        for task_id, epic_id, estimate_min, actual_sec in sessions:
            actual_min = actual_sec / 60.0
            accuracy = actual_min / estimate_min
            total_accuracy += accuracy
            
            if accuracy <= 1.1:
                accurate_count += 1
                status = "✅"
            elif accuracy <= 1.5:
                status = "🟡"
            else:
                status = "🔴"
            
            print(f"{status} {task_id} (Epic {epic_id}): {actual_min:.1f}/{estimate_min}min ({accuracy:.2f}x)")
        
        # Estatísticas gerais
        avg_accuracy = total_accuracy / len(sessions)
        accuracy_rate = (accurate_count / len(sessions)) * 100
        
        print(f"\n📈 Resumo:")
        print(f"   Tasks completadas: {len(sessions)}")
        print(f"   Accuracy média: {avg_accuracy:.2f}x")
        print(f"   Taxa de precisão: {accuracy_rate:.1f}% (dentro de 10%)")


def main():
    """Função principal CLI."""
    parser = argparse.ArgumentParser(
        description="🕒 ETL Debrito - Task Timer com Tracking Temporal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python -m tdah_tools.task_timer start 3.1a
  python -m tdah_tools.task_timer pause
  python -m tdah_tools.task_timer resume
  python -m tdah_tools.task_timer stop
  python -m tdah_tools.task_timer status
  python -m tdah_tools.task_timer report
  python -m tdah_tools.task_timer report --days 14
        """
    )
    
    parser.add_argument(
        "command",
        choices=["start", "pause", "resume", "stop", "status", "report"],
        help="Comando do timer"
    )
    
    parser.add_argument(
        "task_id",
        nargs="?",
        help="ID da task (obrigatório para 'start')"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Dias para relatório (padrão: 7)"
    )
    
    parser.add_argument(
        "--db",
        type=str,
        default="task_timer.db",
        help="Caminho do banco SQLite (padrão: task_timer.db)"
    )
    
    args = parser.parse_args()
    
    timer = TaskTimer(db_path=args.db)
    
    try:
        if args.command == "start":
            if not args.task_id:
                print("❌ Task ID obrigatório para 'start'")
                return 1
            timer.start_task(args.task_id)
            
        elif args.command == "pause":
            timer.pause_task()
            
        elif args.command == "resume":
            timer.resume_task()
            
        elif args.command == "stop":
            timer.stop_task()
            
        elif args.command == "status":
            timer.get_status()
            
        elif args.command == "report":
            timer.generate_report(days=args.days)
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())