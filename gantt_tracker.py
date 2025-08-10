#!/usr/bin/env python3
"""
🎯 ETL Debrito - Gantt Tracker com Parsing Inteligente de Commits
================================================================

Sistema que combina baseline do gantt_schedule.mmd com dados reais
extraídos de commits padronizados para gerar visualização rica.

Uso:
    python gantt_tracker.py                    # Gera HTML e abre browser
    python gantt_tracker.py --output file.html # Salva em arquivo específico
    python gantt_tracker.py --report          # Gera relatório texto
"""

import argparse
import re
import subprocess
import plotly.graph_objects as go
import plotly.offline as pyo
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json


class CommitTracker:
    """Parser inteligente de commits padronizados [EPIC-X]."""
    
    def __init__(self):
        self.commit_pattern = r'\[EPIC-(\d+\.?\d*)\]\s+(\w+):\s+(.*)'
        self.task_pattern = r'Task:\s+([\w.-]+)\s*\|\s*Time:\s+(\d+)min\s*\|\s*Status:\s+(red|green|refactor)'
        
    def get_commit_date(self, commit_hash: str) -> datetime:
        """Obter data do commit."""
        try:
            cmd = f'git log -1 --format="%ad" --date=iso {commit_hash}'
            date_str = subprocess.check_output(cmd, shell=True, text=True).strip().strip('"')
            return datetime.fromisoformat(date_str.split()[0])
        except:
            return datetime.now()
    
    def parse_commits_by_epic(self) -> Dict[str, Dict]:
        """Parse todos os commits seguindo padrão [EPIC-X]."""
        
        epic_data = defaultdict(lambda: {
            'tasks_completed': [],
            'total_time_minutes': 0,
            'total_tasks': 0,
            'first_commit': None,
            'last_commit': None,
            'commit_status': 'pending',
            'tdd_phases': {'red': 0, 'green': 0, 'refactor': 0}
        })
        
        try:
            # Buscar commits com padrão EPIC
            cmd = 'git log --oneline --grep="\\[EPIC-" --since="2025-08-01" --date=iso'
            commits = subprocess.check_output(cmd, shell=True, text=True).strip()
            
            if not commits:
                print("⚠️ Nenhum commit encontrado com padrão [EPIC-X]")
                return dict(epic_data)
            
            for commit_line in commits.split('\n'):
                if not commit_line:
                    continue
                
                commit_hash = commit_line.split()[0]
                
                # Parse header do commit
                match = re.search(self.commit_pattern, commit_line)
                if not match:
                    continue
                
                epic_id, commit_type, description = match.groups()
                commit_date = self.get_commit_date(commit_hash)
                
                # Parse body do commit para task info
                try:
                    cmd_body = f'git log -1 --format="%B" {commit_hash}'
                    body = subprocess.check_output(cmd_body, shell=True, text=True)
                    
                    task_match = re.search(self.task_pattern, body)
                    if task_match:
                        task_id, time_min, tdd_status = task_match.groups()
                        
                        task_info = {
                            'task_id': task_id,
                            'time_minutes': int(time_min),
                            'tdd_status': tdd_status,
                            'commit_type': commit_type,
                            'description': description,
                            'date': commit_date,
                            'commit_hash': commit_hash
                        }
                        
                        epic_data[epic_id]['tasks_completed'].append(task_info)
                        epic_data[epic_id]['total_time_minutes'] += int(time_min)
                        epic_data[epic_id]['total_tasks'] += 1
                        epic_data[epic_id]['tdd_phases'][tdd_status] += 1
                    
                    # Atualizar datas
                    if not epic_data[epic_id]['first_commit']:
                        epic_data[epic_id]['first_commit'] = commit_date
                    epic_data[epic_id]['last_commit'] = commit_date
                    
                    # Status do épico baseado em tipo de commit
                    if commit_type == 'milestone':
                        epic_data[epic_id]['commit_status'] = 'done'
                    elif epic_data[epic_id]['commit_status'] != 'done' and task_match:
                        epic_data[epic_id]['commit_status'] = 'active'
                        
                except Exception as e:
                    print(f"⚠️ Erro ao processar commit {commit_hash}: {e}")
                    continue
        
        except Exception as e:
            print(f"❌ Erro ao buscar commits: {e}")
        
        return dict(epic_data)


class MermaidGanttParser:
    """Parser do arquivo gantt_schedule.mmd (baseline)."""
    
    def __init__(self, mmd_path: str = "docs/gantt_schedule.mmd"):
        self.mmd_path = Path(mmd_path)
        
    def parse_mmd_gantt(self) -> List[Dict]:
        """Parse gantt_schedule.mmd para obter baseline planejado."""
        
        if not self.mmd_path.exists():
            print(f"❌ Arquivo {self.mmd_path} não encontrado!")
            return []
        
        with open(self.mmd_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        epics = []
        current_section = "Unknown"
        
        # Pattern para épicos
        epic_pattern = r'^\s*Epic\s+([\d.]+)[:\s]+(.*?)\s*:(.*?),\s*(\w+),\s*([\d-]+),\s*(\d+)d'
        section_pattern = r'^\s*section\s+(.+)'
        
        for line in content.split('\n'):
            # Detectar seção atual
            section_match = re.match(section_pattern, line.strip())
            if section_match:
                current_section = section_match.group(1)
                continue
            
            # Parse épico
            epic_match = re.match(epic_pattern, line)
            if epic_match:
                epic_id, name, status_tags, code, start_date, duration = epic_match.groups()
                
                # Parse status do .mmd
                status = "pending"
                if "done" in status_tags:
                    status = "done"
                elif "active" in status_tags:
                    status = "active"
                elif "crit" in status_tags:
                    status = "critical"
                
                epic = {
                    'id': epic_id,
                    'name': name.strip(),
                    'section': current_section,
                    'mmd_status': status,
                    'planned_start': datetime.strptime(start_date, "%Y-%m-%d"),
                    'planned_days': int(duration),
                    'planned_end': datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=int(duration)),
                    'estimated_minutes': int(duration) * 6 * 60,  # 6h efetivas por dia
                    
                    # Campos que serão preenchidos com dados de commits
                    'actual_time_minutes': 0,
                    'actual_tasks_done': 0,
                    'commit_status': 'pending',
                    'time_accuracy': None,
                    'tdd_completion': 0,
                    'first_commit_date': None,
                    'last_commit_date': None
                }
                
                epics.append(epic)
        
        return epics


class GanttVisualizer:
    """Gerador de visualização rica com Plotly."""
    
    def __init__(self):
        self.colors = {
            'done': '#10B981',      # Verde
            'active': '#F59E0B',    # Amarelo
            'critical': '#EF4444',  # Vermelho
            'pending': '#94A3B8',   # Cinza
            'planned': '#E0E7FF',   # Azul claro (fundo)
            'milestone': '#8B5CF6'  # Roxo
        }
        
    def create_enhanced_gantt(self, epics: List[Dict]) -> go.Figure:
        """Cria Gantt comparison rico com dados merged."""
        
        fig = go.Figure()
        
        # Agrupar por seção para melhor organização visual
        sections = {}
        for epic in epics:
            section = epic.get('section', 'Unknown')
            if section not in sections:
                sections[section] = []
            sections[section].append(epic)
        
        y_pos = 0
        section_colors = ['#F0F9FF', '#F0FDF4', '#FFF7ED', '#F3E8FF']
        
        for section_idx, (section_name, section_epics) in enumerate(sections.items()):
            section_color = section_colors[section_idx % len(section_colors)]
            
            # Adicionar separador de seção
            if y_pos > 0:
                fig.add_hline(y=y_pos - 0.5, line_dash="dot", line_color="gray", opacity=0.3)
            
            for epic in reversed(section_epics):  # Reverter para ordem cronológica
                epic_name = f"Epic {epic['id']}: {epic['name'][:30]}{'...' if len(epic['name']) > 30 else ''}"
                
                # Barra planejada (baseline do .mmd)
                fig.add_trace(go.Bar(
                    name=f"{epic_name} (Planned)" if y_pos == 0 else "",
                    x=[epic['planned_days']],
                    y=[epic_name],
                    orientation='h',
                    marker=dict(
                        color=self.colors['planned'],
                        opacity=0.6,
                        line=dict(color=self.colors[epic['mmd_status']], width=2)
                    ),
                    width=0.6,
                    offset=-0.2,
                    showlegend=(y_pos == 0),
                    legendgroup="planned",
                    hovertemplate=f"""
                    <b>📋 PLANNED</b><br>
                    Epic: {epic['id']} - {epic['name']}<br>
                    Section: {epic['section']}<br>
                    Duration: {epic['planned_days']} days<br>
                    Start: {epic['planned_start'].strftime('%d/%m/%Y')}<br>
                    End: {epic['planned_end'].strftime('%d/%m/%Y')}<br>
                    Status: {epic['mmd_status']}<br>
                    Estimated: {epic['estimated_minutes']} min
                    <extra></extra>
                    """
                ))
                
                # Barra real (se houver dados de commits)
                if epic['actual_time_minutes'] > 0:
                    # Calcular duração real em dias (baseada no tempo gasto)
                    actual_days = max(1, epic['actual_time_minutes'] / (6 * 60))  # 6h = 1 dia
                    
                    # Cor baseada na accuracy
                    if epic['time_accuracy']:
                        if epic['time_accuracy'] <= 1.1:  # ±10%
                            color = self.colors['done']
                            performance = "✅ On Track"
                        elif epic['time_accuracy'] <= 1.5:  # ±50%
                            color = self.colors['active']
                            performance = "⚠️ Over Budget"
                        else:
                            color = self.colors['critical']
                            performance = "🔴 Significantly Over"
                    else:
                        color = self.colors[epic['commit_status']]
                        performance = epic['commit_status'].title()
                    
                    fig.add_trace(go.Bar(
                        name=f"{epic_name} (Actual)" if y_pos == 0 else "",
                        x=[actual_days],
                        y=[epic_name],
                        orientation='h',
                        marker=dict(color=color, opacity=0.9),
                        width=0.6,
                        offset=0.2,
                        showlegend=(y_pos == 0),
                        legendgroup="actual",
                        hovertemplate=f"""
                        <b>⚡ ACTUAL</b><br>
                        Epic: {epic['id']} - {epic['name']}<br>
                        Tasks Done: {epic['actual_tasks_done']}<br>
                        Time Spent: {epic['actual_time_minutes']} min ({actual_days:.1f} days)<br>
                        Time Accuracy: {epic['time_accuracy']:.2f}x<br>
                        Performance: {performance}<br>
                        TDD Progress: R{epic.get('tdd_phases', {}).get('red', 0)} G{epic.get('tdd_phases', {}).get('green', 0)} Rf{epic.get('tdd_phases', {}).get('refactor', 0)}<br>
                        Commits: {epic['first_commit_date'].strftime('%d/%m') if epic['first_commit_date'] else 'N/A'} → {epic['last_commit_date'].strftime('%d/%m') if epic['last_commit_date'] else 'N/A'}
                        <extra></extra>
                        """
                    ))
                
                # Badge de performance
                if epic['time_accuracy'] and epic['time_accuracy'] <= 1.1:
                    fig.add_annotation(
                        x=epic['planned_days'] + 0.5,
                        y=epic_name,
                        text="🏆",
                        showarrow=False,
                        font=dict(size=16)
                    )
                
                y_pos += 1
        
        # Layout rico e profissional
        fig.update_layout(
            title=dict(
                text="🎯 ETL Debrito - Enhanced Gantt Progress Tracker",
                font=dict(size=20, family="Inter, -apple-system, sans-serif")
            ),
            template="plotly_white",
            xaxis=dict(
                title="Days",
                showgrid=True,
                gridcolor='#E5E7EB',
                zeroline=False
            ),
            yaxis=dict(
                title="Epics by Section",
                showgrid=False,
                zeroline=False,
                autorange="reversed"  # Mostrar em ordem cronológica
            ),
            height=max(400, len(epics) * 60 + 150),
            bargap=0.3,
            barmode='overlay',
            hovermode='closest',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor="rgba(255,255,255,0.8)"
            ),
            font=dict(family="Inter, -apple-system, sans-serif", size=11),
            plot_bgcolor='white',
            paper_bgcolor='#F8FAFC'
        )
        
        # Linha "Today" - remover temporariamente por compatibilidade plotly
        # fig.add_vline(
        #     x=datetime.now(),
        #     line_dash="dash", 
        #     line_color="#EF4444",
        #     annotation_text="TODAY",
        #     annotation_position="top"
        # )
        
        return fig
    
    def create_summary_metrics(self, epics: List[Dict]) -> str:
        """Gera métricas summary em HTML."""
        
        total_epics = len(epics)
        completed_epics = len([e for e in epics if e['commit_status'] == 'done'])
        active_epics = len([e for e in epics if e['actual_time_minutes'] > 0 and e['commit_status'] != 'done'])
        
        # Cálculos de accuracy
        epics_with_data = [e for e in epics if e['time_accuracy']]
        if epics_with_data:
            avg_accuracy = sum(e['time_accuracy'] for e in epics_with_data) / len(epics_with_data)
            on_track = len([e for e in epics_with_data if e['time_accuracy'] <= 1.1])
            accuracy_grade = "A" if avg_accuracy <= 1.1 else "B" if avg_accuracy <= 1.3 else "C"
            accuracy_color = "green" if accuracy_grade == "A" else "orange" if accuracy_grade == "B" else "red"
        else:
            avg_accuracy = 0
            on_track = 0
            accuracy_grade = "N/A"
            accuracy_color = "gray"
        
        # Total de tempo gasto
        total_time_minutes = sum(e['actual_time_minutes'] for e in epics)
        total_tasks_done = sum(e['actual_tasks_done'] for e in epics)
        
        # TDD statistics
        total_tdd = defaultdict(int)
        for epic in epics:
            for phase, count in epic.get('tdd_phases', {}).items():
                total_tdd[phase] += count
        
        return f"""
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0;">
            <div style="background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #3B82F6;">
                <h3 style="margin: 0 0 15px 0; color: #1E293B;">📊 Epic Progress</h3>
                <div style="font-size: 28px; font-weight: bold; color: #3B82F6; margin-bottom: 5px;">{completed_epics}/{total_epics}</div>
                <div style="color: #64748B;">Completed Epics</div>
                <div style="margin-top: 10px; color: #64748B; font-size: 14px;">Active: {active_epics}</div>
            </div>
            
            <div style="background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid {accuracy_color};">
                <h3 style="margin: 0 0 15px 0; color: #1E293B;">🎯 Time Accuracy</h3>
                <div style="font-size: 28px; font-weight: bold; color: {accuracy_color}; margin-bottom: 5px;">{avg_accuracy:.2f}x</div>
                <div style="color: #64748B;">Average (Grade {accuracy_grade})</div>
                <div style="margin-top: 10px; color: #64748B; font-size: 14px;">On Track: {on_track}/{len(epics_with_data)} ({on_track/len(epics_with_data)*100 if epics_with_data else 0:.0f}%)</div>
            </div>
            
            <div style="background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #10B981;">
                <h3 style="margin: 0 0 15px 0; color: #1E293B;">⏱️ Time Invested</h3>
                <div style="font-size: 28px; font-weight: bold; color: #10B981; margin-bottom: 5px;">{total_time_minutes}</div>
                <div style="color: #64748B;">Minutes ({total_time_minutes/60:.1f} hours)</div>
                <div style="margin-top: 10px; color: #64748B; font-size: 14px;">Tasks: {total_tasks_done}</div>
            </div>
            
            <div style="background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #8B5CF6;">
                <h3 style="margin: 0 0 15px 0; color: #1E293B;">🧪 TDD Cycles</h3>
                <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 5px;">
                    <span style="color: #EF4444; font-weight: bold;">🔴 {total_tdd['red']}</span>
                    <span style="color: #10B981; font-weight: bold;">🟢 {total_tdd['green']}</span>
                    <span style="color: #F59E0B; font-weight: bold;">🟡 {total_tdd['refactor']}</span>
                </div>
                <div style="color: #64748B; font-size: 14px;">Red-Green-Refactor cycles</div>
            </div>
        </div>
        """


class GanttTracker:
    """Classe principal que orquestra todo o sistema."""
    
    def __init__(self):
        self.commit_tracker = CommitTracker()
        self.mmd_parser = MermaidGanttParser()
        self.visualizer = GanttVisualizer()
    
    def merge_data_sources(self, mmd_epics: List[Dict], commit_data: Dict[str, Dict]) -> List[Dict]:
        """Merge dados do .mmd com dados dos commits."""
        
        for epic in mmd_epics:
            epic_id = epic['id']
            
            if epic_id in commit_data:
                commit_info = commit_data[epic_id]
                
                # Merge dados de commits
                epic['actual_time_minutes'] = commit_info['total_time_minutes']
                epic['actual_tasks_done'] = commit_info['total_tasks']
                epic['commit_status'] = commit_info['commit_status']
                epic['tdd_phases'] = commit_info['tdd_phases']
                epic['first_commit_date'] = commit_info['first_commit']
                epic['last_commit_date'] = commit_info['last_commit']
                
                # Calcular accuracy
                if epic['actual_time_minutes'] > 0:
                    epic['time_accuracy'] = epic['actual_time_minutes'] / epic['estimated_minutes']
                
                # TDD completion percentage
                total_tdd = sum(commit_info['tdd_phases'].values())
                if total_tdd > 0:
                    epic['tdd_completion'] = (commit_info['tdd_phases']['green'] + 
                                            commit_info['tdd_phases']['refactor']) / total_tdd * 100
                
                # Status final (commit status overrides .mmd se mais avançado)
                if commit_info['commit_status'] == 'done':
                    epic['final_status'] = 'done'
                elif commit_info['commit_status'] == 'active':
                    epic['final_status'] = 'active'
                else:
                    epic['final_status'] = epic['mmd_status']
            else:
                epic['final_status'] = epic['mmd_status']
        
        return mmd_epics
    
    def generate_html_report(self, output_path: str = "gantt_progress.html") -> Path:
        """Gera relatório HTML completo."""
        
        print("🔍 Parsing commits with [EPIC-X] pattern...")
        commit_data = self.commit_tracker.parse_commits_by_epic()
        
        print("📋 Loading baseline from gantt_schedule.mmd...")
        mmd_epics = self.mmd_parser.parse_mmd_gantt()
        
        print("🔄 Merging data sources...")
        enhanced_epics = self.merge_data_sources(mmd_epics, commit_data)
        
        print("📊 Creating enhanced Gantt visualization...")
        fig = self.visualizer.create_enhanced_gantt(enhanced_epics)
        chart_html = pyo.plot(fig, output_type='div', include_plotlyjs='cdn')
        
        print("📈 Generating summary metrics...")
        summary_html = self.visualizer.create_summary_metrics(enhanced_epics)
        
        # Template HTML completo
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>ETL Debrito - Gantt Progress Tracker</title>
            <style>
                body {{
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    margin: 0;
                    padding: 20px;
                    min-height: 100vh;
                }}
                .container {{
                    max-width: 1400px;
                    margin: 0 auto;
                    background: rgba(255, 255, 255, 0.95);
                    border-radius: 16px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #1E293B 0%, #334155 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 2.5rem;
                    font-weight: 700;
                    text-shadow: 0 2px 4px rgba(0,0,0,0.3);
                }}
                .header p {{
                    margin: 10px 0 0 0;
                    opacity: 0.9;
                    font-size: 1.1rem;
                }}
                .content {{
                    padding: 30px;
                }}
                .footer {{
                    background: #F1F5F9;
                    padding: 20px 30px;
                    text-align: center;
                    color: #64748B;
                    border-top: 1px solid #E2E8F0;
                }}
                .badge {{
                    display: inline-block;
                    background: rgba(59, 130, 246, 0.1);
                    color: #3B82F6;
                    padding: 4px 8px;
                    border-radius: 6px;
                    font-size: 0.875rem;
                    font-weight: 600;
                    margin: 0 4px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎯 ETL Debrito</h1>
                    <p>Enhanced Gantt Progress Tracker</p>
                    <div style="margin-top: 15px;">
                        <span class="badge">Real-time Commit Tracking</span>
                        <span class="badge">TDD Cycle Monitoring</span>
                        <span class="badge">Performance Analytics</span>
                    </div>
                </div>
                
                <div class="content">
                    <div style="text-align: center; margin-bottom: 30px; color: #64748B;">
                        Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')} | 
                        Based on commits with [EPIC-X] pattern
                    </div>
                    
                    {summary_html}
                    
                    <div style="margin: 30px 0;">
                        {chart_html}
                    </div>
                </div>
                
                <div class="footer">
                    <div>
                        🤖 <strong>Auto-generated</strong> from 
                        <code>gantt_schedule.mmd</code> baseline + git commit analysis
                    </div>
                    <div style="margin-top: 10px; font-size: 0.875rem;">
                        Commit Pattern: <code>[EPIC-X] type: description</code> | 
                        Task Pattern: <code>Task: ID | Time: Xmin | Status: red/green/refactor</code>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        output_file = Path(output_path)
        
        # Criar diretório se não existir
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        output_file.write_text(html_content, encoding='utf-8')
        
        return output_file
    
    def generate_text_report(self) -> str:
        """Gera relatório texto para CLI/logs."""
        
        commit_data = self.commit_tracker.parse_commits_by_epic()
        mmd_epics = self.mmd_parser.parse_mmd_gantt()
        enhanced_epics = self.merge_data_sources(mmd_epics, commit_data)
        
        report_lines = [
            "=" * 60,
            "🎯 ETL DEBRITO - GANTT PROGRESS REPORT",
            "=" * 60,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        for epic in enhanced_epics:
            status_icon = "✅" if epic['final_status'] == 'done' else "🔄" if epic['actual_time_minutes'] > 0 else "⏸️"
            accuracy_text = f" ({epic['time_accuracy']:.2f}x)" if epic['time_accuracy'] else ""
            
            report_lines.extend([
                f"{status_icon} Epic {epic['id']}: {epic['name']}",
                f"   Section: {epic['section']}",
                f"   Planned: {epic['planned_days']} days ({epic['estimated_minutes']} min)",
                f"   Actual: {epic['actual_time_minutes']} min{accuracy_text}",
                f"   Tasks Done: {epic['actual_tasks_done']}",
                f"   TDD: R{epic.get('tdd_phases', {}).get('red', 0)} G{epic.get('tdd_phases', {}).get('green', 0)} Rf{epic.get('tdd_phases', {}).get('refactor', 0)}",
                ""
            ])
        
        return "\n".join(report_lines)


def main():
    """CLI principal."""
    parser = argparse.ArgumentParser(
        description="🎯 ETL Debrito - Gantt Tracker com Parsing Inteligente",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python gantt_tracker.py                           # Gera HTML e abre no browser
  python gantt_tracker.py --output custom.html      # Salva em arquivo específico  
  python gantt_tracker.py --report                  # Gera relatório texto
  python gantt_tracker.py --report --output -       # Relatório para stdout
        """
    )
    
    parser.add_argument(
        "--output", "-o",
        default="docs/gantt_progress.html",
        help="Arquivo de saída (default: docs/gantt_progress.html)"
    )
    
    parser.add_argument(
        "--report", "-r",
        action="store_true",
        help="Gerar relatório texto em vez de HTML"
    )
    
    parser.add_argument(
        "--no-open",
        action="store_true", 
        help="Não abrir automaticamente no browser"
    )
    
    args = parser.parse_args()
    
    tracker = GanttTracker()
    
    try:
        if args.report:
            print("📊 Generating text report...")
            report = tracker.generate_text_report()
            
            if args.output == "-":
                print(report)
            else:
                with open(args.output, 'w') as f:
                    f.write(report)
                print(f"📄 Text report saved: {args.output}")
        else:
            print("🎯 ETL Debrito - Enhanced Gantt Tracker")
            print("=" * 50)
            
            output_file = tracker.generate_html_report(args.output)
            
            print(f"✅ Enhanced Gantt generated: {output_file}")
            
            if not args.no_open:
                import webbrowser
                webbrowser.open(f"file://{output_file.absolute()}")
                print("🌐 Opening in browser...")
            
            print("\n🎮 Features included:")
            print("  • Baseline from gantt_schedule.mmd")
            print("  • Real progress from [EPIC-X] commits")
            print("  • Time accuracy metrics")
            print("  • TDD cycle tracking") 
            print("  • Performance analytics")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())