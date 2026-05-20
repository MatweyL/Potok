const formatDuration = (totalSeconds) => {
    if (totalSeconds < 0) return '0 секунд';
    if (totalSeconds === 0) return '0 секунд';

    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    const parts = [];

    if (days > 0) {
        parts.push(formatPart(days, ['день', 'дня', 'дней']));
    }
    if (hours > 0) {
        parts.push(formatPart(hours, ['час', 'часа', 'часов']));
    }
    if (minutes > 0) {
        parts.push(formatPart(minutes, ['минута', 'минуты', 'минут']));
    }
    if (seconds > 0 || parts.length === 0) {
        parts.push(formatPart(seconds, ['секунда', 'секунды', 'секунд']));
    }

    return parts.join(' ');
};

const formatPart = (number, words) => {
    const cases = [2, 0, 1, 1, 1, 2];
    const index = (number % 100 > 4 && number % 100 < 20)
        ? 2
        : cases[Math.min(number % 10, 5)];
    return `${number} ${words[index]}`;
};
