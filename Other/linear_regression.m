%% Initialization
clear ; close all; clc

%% =========== Part 1: Loading and Visualizing Data =============
%  We start the exercise by first loading and visualizing the dataset. 
%  The following code will load the dataset into your environment and plot
%  the data.

% Load from ex5data1: 
% You will have X, y, Xval, yval, Xtest, ytest in your environment

%load('.mat');


% m = Number of examples
m = size(X_poly, 1);
n = size(X_poly, 2);

if(n == 6)
    X_poly(:,5) = log2(X_poly(:,5)./1024./1024);
end

if(n == 7)
    X_poly(:,6) = log2(X_poly(:,6)./1024./1024);
end

[X_poly, mu, sigma] = featureNormalize(X_poly);  % Normalize
X_poly = [ones(m, 1), X_poly];                   % Add Ones

lambda = 1;
[theta] = trainLinearReg(X_poly, y, lambda);


% % Plot training data and fit
% plot(X, y, 'rx', 'MarkerSize', 10, 'LineWidth', 1.5);
% plotFit(min(X), max(X), mu, sigma, theta, p);
% xlabel('Change in water level (x)');
% ylabel('Water flowing out of the dam (y)');
% title (sprintf('Polynomial Regression Fit (lambda = %f)', lambda));
