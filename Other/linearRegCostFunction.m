function [J, grad] = linearRegCostFunction(X, y, theta, lambda)

m = length(y); % number of training examples

% ====================== YOUR CODE HERE ======================

h_theta  = X * theta;   

J = 1 / 2 / m * sum((h_theta - y).^2) + lambda / 2 / m * sum(theta(2:end,:).^2); 

grad = 1/m *  X' * (h_theta - y) + lambda / m .* theta;

grad(1,1) = grad(1,1) - lambda / m * theta(1,1);


% =========================================================================

grad = grad(:);

end
