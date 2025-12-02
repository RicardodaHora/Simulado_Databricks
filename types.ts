export interface Question {
  pergunta: string;
  alternativas: string[];
  resposta: string;
}

export enum QuizState {
  INTRO = 'INTRO',
  PLAYING = 'PLAYING',
  FINISHED = 'FINISHED'
}

export interface QuizResult {
  total: number;
  correct: number;
  wrong: number;
}