import React, { useState, useMemo, useEffect } from 'react';
import { RAW_DATA } from './data';
import { Question, QuizState, QuizResult } from './types';
import { ChartPie, CheckCircle, XCircle, ArrowRight, RotateCcw, Award } from 'lucide-react';

const QUESTIONS_PER_SESSION = 45;

const App: React.FC = () => {
  const [gameState, setGameState] = useState<QuizState>(QuizState.INTRO);
  const [activeQuestions, setActiveQuestions] = useState<Question[]>([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [selectedOptionIndex, setSelectedOptionIndex] = useState<number | null>(null);
  const [isAnswerRevealed, setIsAnswerRevealed] = useState(false);
  const [score, setScore] = useState(0);

  // Shuffle and pick questions on game start
  const startQuiz = () => {
    const shuffled = [...RAW_DATA].sort(() => 0.5 - Math.random());
    setActiveQuestions(shuffled.slice(0, QUESTIONS_PER_SESSION));
    setCurrentIndex(0);
    setScore(0);
    setGameState(QuizState.PLAYING);
    resetQuestionState();
  };

  const resetQuestionState = () => {
    setSelectedOptionIndex(null);
    setIsAnswerRevealed(false);
  };

  const handleOptionSelect = (index: number) => {
    if (isAnswerRevealed) return;
    setSelectedOptionIndex(index);
  };

  const confirmAnswer = () => {
    if (selectedOptionIndex === null) return;
    
    setIsAnswerRevealed(true);
    const currentQ = activeQuestions[currentIndex];
    const letter = ['A', 'B', 'C', 'D', 'E'][selectedOptionIndex];
    
    // Logic: The answer provided in JSON is usually a single letter (e.g., "A").
    // Sometimes it might have whitespace.
    const cleanAnswer = currentQ.resposta.trim().toUpperCase();
    
    if (letter === cleanAnswer) {
      setScore(s => s + 1);
    }
  };

  const nextQuestion = () => {
    if (currentIndex < activeQuestions.length - 1) {
      setCurrentIndex(prev => prev + 1);
      resetQuestionState();
    } else {
      setGameState(QuizState.FINISHED);
    }
  };

  // Helper to determine style of options based on game state
  const getOptionStyle = (index: number, optionLetter: string, correctLetter: string) => {
    const isSelected = selectedOptionIndex === index;
    const isCorrect = optionLetter === correctLetter;

    if (!isAnswerRevealed) {
      if (isSelected) return "bg-blue-100 border-blue-500 text-blue-900 font-medium";
      return "bg-white border-slate-200 hover:bg-slate-50 text-slate-700";
    }

    // Revealed state
    if (isCorrect) {
      return "bg-green-100 border-green-500 text-green-900 font-medium ring-1 ring-green-500";
    }
    if (isSelected && !isCorrect) {
      return "bg-red-100 border-red-500 text-red-900 opacity-60";
    }
    
    return "bg-white border-slate-200 text-slate-400 opacity-60";
  };

  // --------------------------------------------------------------------------------
  // SCREENS
  // --------------------------------------------------------------------------------

  // 1. Intro Screen
  if (gameState === QuizState.INTRO) {
    return (
      <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center p-6 text-center space-y-8">
        <div className="space-y-4">
          <div className="bg-white p-6 rounded-2xl shadow-xl border border-slate-100 mx-auto w-32 h-32 flex items-center justify-center">
             <div className="bg-red-500 rounded-lg p-2">
                 <img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Logo" className="w-16 h-16 object-contain filter brightness-0 invert" />
             </div>
          </div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Simulado Databricks</h1>
          <p className="text-slate-500 max-w-xs mx-auto">
            Prepare-se para a certificação com {QUESTIONS_PER_SESSION} questões aleatórias.
          </p>
        </div>

        <button 
          onClick={startQuiz}
          className="w-full max-w-sm bg-red-600 active:bg-red-700 text-white font-semibold py-4 px-8 rounded-xl shadow-lg shadow-red-200 transform transition active:scale-95 flex items-center justify-center gap-3 touch-manipulation"
        >
          <span>Iniciar Simulado</span>
          <ArrowRight className="w-5 h-5" />
        </button>

        <div className="text-xs text-slate-400">
          Baseado em dados oficiais
        </div>
      </div>
    );
  }

  // 2. Finished Screen
  if (gameState === QuizState.FINISHED) {
    const percentage = Math.round((score / QUESTIONS_PER_SESSION) * 100);
    const isPass = percentage >= 70;

    return (
      <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center p-6 text-center space-y-8">
        <div className="space-y-2">
           <Award className={`w-20 h-20 mx-auto ${isPass ? 'text-green-500' : 'text-amber-500'}`} />
          <h2 className="text-2xl font-bold text-slate-900">Resultado Final</h2>
        </div>

        <div className="bg-white rounded-2xl p-8 w-full max-w-sm shadow-sm border border-slate-100 space-y-6">
          <div className="text-6xl font-black text-slate-900 tracking-tighter">
            {score}<span className="text-2xl text-slate-400 font-normal">/{QUESTIONS_PER_SESSION}</span>
          </div>
          
          <div className="space-y-2">
            <div className="h-4 w-full bg-slate-100 rounded-full overflow-hidden">
              <div 
                className={`h-full rounded-full ${isPass ? 'bg-green-500' : 'bg-amber-500'}`} 
                style={{ width: `${percentage}%` }}
              ></div>
            </div>
            <div className="flex justify-between text-sm font-medium">
              <span className={isPass ? 'text-green-600' : 'text-amber-600'}>
                {percentage}% de acerto
              </span>
              <span className="text-slate-400">70% para passar</span>
            </div>
          </div>
        </div>

        <button 
          onClick={startQuiz}
          className="w-full max-w-sm bg-slate-900 active:bg-black text-white font-semibold py-4 px-8 rounded-xl shadow-lg transform transition active:scale-95 flex items-center justify-center gap-2"
        >
          <RotateCcw className="w-5 h-5" />
          <span>Tentar Novamente</span>
        </button>
      </div>
    );
  }

  // 3. Playing Screen
  const currentQ = activeQuestions[currentIndex];
  const progress = ((currentIndex + 1) / QUESTIONS_PER_SESSION) * 100;
  const correctLetter = currentQ.resposta.trim().toUpperCase();

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 px-4 py-3 sticky top-0 z-10 shadow-sm">
        <div className="max-w-xl mx-auto flex items-center justify-between">
          <div className="flex flex-col">
            <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Questão {currentIndex + 1} de {QUESTIONS_PER_SESSION}</span>
            <div className="h-1.5 w-32 bg-slate-100 rounded-full mt-1 overflow-hidden">
              <div className="h-full bg-red-500 rounded-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
            </div>
          </div>
          <div className="bg-slate-100 text-slate-600 px-3 py-1 rounded-full text-xs font-bold">
            Score: {score}
          </div>
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 px-4 py-6 max-w-xl mx-auto w-full pb-32">
        <div className="space-y-6">
          {/* Question Text */}
          <h2 className="text-lg font-semibold text-slate-800 leading-snug whitespace-pre-wrap">
            {currentQ.pergunta}
          </h2>

          {/* Options */}
          <div className="space-y-3">
            {currentQ.alternativas.map((alt, idx) => {
              const letter = ['A', 'B', 'C', 'D', 'E'][idx];
              // Remove the "A. " prefix if it exists in the text for cleaner UI
              const cleanText = alt.replace(/^[A-E]\.\s*/, '');
              
              return (
                <button
                  key={idx}
                  onClick={() => handleOptionSelect(idx)}
                  disabled={isAnswerRevealed}
                  className={`
                    w-full text-left p-4 rounded-xl border-2 transition-all duration-200 flex items-start gap-3
                    ${getOptionStyle(idx, letter, correctLetter)}
                  `}
                >
                  <div className={`
                    flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold border
                    ${selectedOptionIndex === idx ? 'border-current' : 'border-slate-300 text-slate-400'}
                  `}>
                    {letter}
                  </div>
                  <span className="text-sm pt-1 leading-relaxed">{cleanText}</span>
                </button>
              );
            })}
          </div>
        </div>
      </main>

      {/* Footer / Actions */}
      <footer className="fixed bottom-0 left-0 right-0 p-4 bg-white border-t border-slate-200 backdrop-blur-md bg-opacity-90">
        <div className="max-w-xl mx-auto">
          {!isAnswerRevealed ? (
            <button
              onClick={confirmAnswer}
              disabled={selectedOptionIndex === null}
              className={`
                w-full py-4 rounded-xl font-bold text-lg shadow-lg transition-all
                ${selectedOptionIndex === null 
                  ? 'bg-slate-200 text-slate-400 cursor-not-allowed' 
                  : 'bg-red-600 text-white active:scale-95 shadow-red-200'}
              `}
            >
              Confirmar
            </button>
          ) : (
            <div className="space-y-3 animate-in fade-in slide-in-from-bottom-4 duration-300">
              <div className={`p-4 rounded-xl flex items-center gap-3 ${
                ['A', 'B', 'C', 'D', 'E'][selectedOptionIndex!] === correctLetter
                  ? 'bg-green-50 text-green-700 border border-green-100'
                  : 'bg-red-50 text-red-700 border border-red-100'
              }`}>
                {['A', 'B', 'C', 'D', 'E'][selectedOptionIndex!] === correctLetter 
                  ? <CheckCircle className="w-6 h-6 shrink-0" />
                  : <XCircle className="w-6 h-6 shrink-0" />
                }
                <div className="font-medium text-sm">
                  {['A', 'B', 'C', 'D', 'E'][selectedOptionIndex!] === correctLetter 
                    ? "Correto! Muito bem."
                    : `Incorreto. A resposta certa é ${correctLetter}.`
                  }
                </div>
              </div>
              <button
                onClick={nextQuestion}
                className="w-full bg-slate-900 text-white py-4 rounded-xl font-bold text-lg shadow-lg active:scale-95 transition-all"
              >
                {currentIndex < activeQuestions.length - 1 ? 'Próxima Questão' : 'Ver Resultado'}
              </button>
            </div>
          )}
        </div>
      </footer>
    </div>
  );
};

export default App;